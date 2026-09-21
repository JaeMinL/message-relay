package com.example.relay;

import com.example.relay.proto.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One registered client stream.
 *
 * <p>gRPC forbids concurrent onNext calls on a single StreamObserver, so every
 * write goes through {@link #writeLock}. The lock also guards
 * {@link #inFlightSeqNo}, which is how FIFO is enforced: at most one message per
 * recipient is on the wire at a time, and the next one is only pumped after an ACK.
 */
public final class ClientConnection {

    private final String clientId;
    private final StreamObserver<ServerEvent> observer;
    private final ServerCallStreamObserver<ServerEvent> flowControl;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int maxOutboundBufferSize;

    /** Sequence number awaiting an ACK, or null when the client is idle. */
    private Long inFlightSeqNo;
    /** Writes attempted while the transport buffer was full. */
    private int queuedWhileNotReady;

    public ClientConnection(final String clientId,
                            final StreamObserver<ServerEvent> observer,
                            final int maxOutboundBufferSize) {
        this.clientId = clientId;
        this.observer = observer;
        this.maxOutboundBufferSize = maxOutboundBufferSize;
        this.flowControl = observer instanceof ServerCallStreamObserver<ServerEvent> s ? s : null;
    }

    public String clientId() {
        return clientId;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public ReentrantLock writeLock() {
        return writeLock;
    }

    /** Caller must hold {@link #writeLock}. */
    public Long inFlightSeqNo() {
        return inFlightSeqNo;
    }

    /** Caller must hold {@link #writeLock}. */
    public void setInFlightSeqNo(final Long seqNo) {
        this.inFlightSeqNo = seqNo;
    }

    /**
     * Sends an event, taking the write lock.
     *
     * @return false if the connection was already closed or the write failed
     * @throws OutboundOverflowException when the client is too slow and the
     *         per-connection outbound allowance is exhausted
     */
    public boolean send(final ServerEvent event) {
        writeLock.lock();
        try {
            return sendLocked(event);
        } finally {
            writeLock.unlock();
        }
    }

    /** Caller must already hold {@link #writeLock}. */
    public boolean sendLocked(final ServerEvent event) {
        if (closed.get()) {
            return false;
        }
        if (flowControl != null) {
            if (flowControl.isCancelled()) {
                closed.set(true);
                return false;
            }
            if (!flowControl.isReady()) {
                // A slow consumer must not be allowed to grow the Netty write
                // queue without bound, and must not stall other clients.
                if (++queuedWhileNotReady > maxOutboundBufferSize) {
                    throw new OutboundOverflowException(clientId);
                }
            } else {
                queuedWhileNotReady = 0;
            }
        }
        try {
            observer.onNext(event);
            return true;
        } catch (final StatusRuntimeException | IllegalStateException e) {
            // The peer went away between our readiness check and the write.
            closed.set(true);
            return false;
        }
    }

    /** Completes the stream normally. Idempotent. */
    public void complete() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        writeLock.lock();
        try {
            observer.onCompleted();
        } catch (final RuntimeException ignored) {
            // Already torn down by the transport.
        } finally {
            writeLock.unlock();
        }
    }

    /** Terminates the stream with a gRPC status. Idempotent. */
    public void closeWithStatus(final Status status) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        writeLock.lock();
        try {
            observer.onError(status.asRuntimeException());
        } catch (final RuntimeException ignored) {
            // Already torn down by the transport.
        } finally {
            writeLock.unlock();
        }
    }

    /** Signals that the transport is gone; no further writes are attempted. */
    public void onClientDisconnected() {
        closed.set(true);
    }

    /** Raised when a slow client exceeds its outbound allowance. */
    public static final class OutboundOverflowException extends RuntimeException {
        public OutboundOverflowException(final String clientId) {
            super("outbound buffer exhausted for client " + clientId);
        }
    }
}
