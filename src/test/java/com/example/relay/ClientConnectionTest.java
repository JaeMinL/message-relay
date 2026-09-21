package com.example.relay;

import com.example.relay.proto.ServerEvent;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ClientConnectionTest {
    private final ServerEvent event = ServerEvent.getDefaultInstance();

    @Test
    void closedOrCancelledConnectionsRejectWrites() {
        // given: a transport cancelled by the peer
        final ServerCallStreamObserver<ServerEvent> observer = mock(ServerCallStreamObserver.class);
        when(observer.isCancelled()).thenReturn(true);
        final ClientConnection connection = new ClientConnection("bob", observer, 1);
        // when: delivery is attempted twice
        final boolean first = connection.send(event);
        final boolean second = connection.send(event);
        // then: cancellation is remembered and nothing is written
        assertFalse(first);
        assertFalse(second);
        assertTrue(connection.isClosed());
        verify(observer, never()).onNext(any());
    }

    @Test
    void readinessResetsTheOutboundAllowance() {
        // given: a transport with room for one write while not ready
        final ServerCallStreamObserver<ServerEvent> observer = mock(ServerCallStreamObserver.class);
        when(observer.isReady()).thenReturn(false, true, false, false);
        final ClientConnection connection = new ClientConnection("bob", observer, 1);
        // when: readiness recovers before becoming blocked again
        assertTrue(connection.send(event));
        assertTrue(connection.send(event));
        assertTrue(connection.send(event));
        // then: only the second consecutive blocked write overflows, and the lock is released
        final var failure = assertThrows(ClientConnection.OutboundOverflowException.class,
                () -> connection.send(event));
        assertTrue(failure.getMessage().contains("bob"));
        assertFalse(connection.writeLock().isLocked());
        verify(observer, times(3)).onNext(event);
    }

    @Test
    void writeFailuresCloseTheConnection() {
        // given: observers that reject writes after transport teardown
        for (final RuntimeException failure : new RuntimeException[]{
                Status.CANCELLED.asRuntimeException(), new IllegalStateException("closed")}) {
            final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
            doThrow(failure).when(observer).onNext(any());
            final ClientConnection connection = new ClientConnection("bob", observer, 1);
            // when: sending encounters the transport failure
            final boolean sent = connection.send(event);
            // then: the failure is reported without leaking the write lock
            assertFalse(sent);
            assertTrue(connection.isClosed());
            assertFalse(connection.writeLock().isLocked());
        }
    }

    @Test
    void terminalCallbacksAreIdempotentEvenWhenObserversThrow() {
        // given: normal and already-torn-down observers
        for (final boolean throwsOnClose : new boolean[]{false, true}) {
            final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
            if (throwsOnClose) {
                doThrow(new IllegalStateException("gone")).when(observer).onCompleted();
                doThrow(new IllegalStateException("gone")).when(observer).onError(any());
            }
            final ClientConnection completed = new ClientConnection("completed", observer, 1);
            final ClientConnection failed = new ClientConnection("failed", observer, 1);
            // when: terminal callbacks are requested repeatedly
            completed.complete();
            completed.complete();
            failed.closeWithStatus(Status.UNAVAILABLE);
            failed.closeWithStatus(Status.UNAVAILABLE);
            // then: each observer callback occurs once and locks are released
            verify(observer).onCompleted();
            verify(observer).onError(argThat(error -> Status.fromThrowable(error).getCode() == Status.Code.UNAVAILABLE));
            assertTrue(completed.isClosed());
            assertTrue(failed.isClosed());
            assertFalse(completed.writeLock().isLocked());
            assertFalse(failed.writeLock().isLocked());
        }
    }
}
