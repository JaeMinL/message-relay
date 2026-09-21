package com.example.relay;

import com.example.relay.proto.Ack;
import com.example.relay.proto.ClientEvent;
import com.example.relay.proto.MessageRelayGrpc;
import com.example.relay.proto.ProtocolError;
import com.example.relay.proto.ProtocolErrorCode;
import com.example.relay.proto.Register;
import com.example.relay.proto.Registered;
import com.example.relay.proto.Send;
import com.example.relay.proto.SendAccepted;
import com.example.relay.proto.SendRejected;
import com.example.relay.proto.ServerEvent;
import com.example.relay.store.StorageException;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC entry point. One bidirectional stream per client.
 *
 * Requires registration before Send or Ack and reports validation failures without
 * closing the stream. gRPC serializes inbound events; DeliveryService coordinates
 * per-recipient delivery state.
 */
public final class RelayServiceImpl extends MessageRelayGrpc.MessageRelayImplBase {

    private static final Logger log = LoggerFactory.getLogger(RelayServiceImpl.class);

    private final RelayConfig config;
    private final ConnectionRegistry registry;
    private final DeliveryService deliveryService;

    public RelayServiceImpl(final RelayConfig config,
                            final ConnectionRegistry registry,
                            final DeliveryService deliveryService) {
        this.config = config;
        this.registry = registry;
        this.deliveryService = deliveryService;
    }

    @Override
    public StreamObserver<ClientEvent> connect(final StreamObserver<ServerEvent> responseObserver) {
        return new ClientStream(responseObserver);
    }

    /** Per-call state machine: unregistered -> registered -> closed. */
    private final class ClientStream implements StreamObserver<ClientEvent> {

        private final StreamObserver<ServerEvent> responseObserver;
        private ClientConnection connection;

        ClientStream(final StreamObserver<ServerEvent> responseObserver) {
            this.responseObserver = responseObserver;

            if (responseObserver instanceof ServerCallStreamObserver<ServerEvent> serverCallObserver) {
                // Resume pumping when the transport drains, so a briefly slow client
                // recovers instead of sitting idle until the retry scan.
                serverCallObserver.setOnReadyHandler(() -> {
                    final ClientConnection clientConnection = connection;
                    if (clientConnection != null && !clientConnection.isClosed()) {
                        deliveryService.deliverNextMessageIfReady(clientConnection);
                    }
                });
            }
        }

        @Override
        public void onNext(final ClientEvent event) {
            try {
                switch (event.getEventCase()) {
                    case REGISTER -> handleRegister(event.getRegister());
                    case SEND -> handleSend(event.getSend());
                    case ACK -> handleAck(event.getAck());
                    case EVENT_NOT_SET -> handleUnsetEvent();
                }
            } catch (final ClientConnection.OutboundOverflowException e) {
                // A client that will not read cannot be allowed to hold memory.
                closeStream(Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()));
            } catch (final RuntimeException e) {
                log.error("unhandled error while processing a client event", e);
                closeStream(Status.INTERNAL.withDescription("internal error"));
            }
        }

        private void handleRegister(final Register register) {
            if (connection != null) {
                // Non-fatal: the stream is already usable, the client just repeated itself.
                sendProtocolError(ProtocolErrorCode.ALREADY_REGISTERED,
                        false,
                        false,
                        "this stream is already registered as " + connection.clientId());
                return;
            }
            final String clientId = register.getClientId();
            if (!deliveryService.isValidClientId(clientId))
            {
                sendProtocolError(
                        ProtocolErrorCode.INVALID_REGISTRATION,
                        false,
                        false,
                        "client_id must be non-blank and at most " + config.maxClientIdSize + " bytes"
                );
                return;
            }

            final ClientConnection candidate = new ClientConnection(clientId, responseObserver, config.maxOutboundBufferSize);

            switch (registry.register(candidate)) {
                case ConnectionRegistry.RegisterOutcome.LimitReached ignored -> {
                    sendProtocolError(
                            ProtocolErrorCode.CONNECTION_LIMIT_REACHED,
                            true,
                            false,
                            "the server is at its " + config.maxActiveSessions + " concurrent connection limit"
                    );
                    return;
                }
                case ConnectionRegistry.RegisterOutcome.DuplicateId ignored -> {
                    sendProtocolError(ProtocolErrorCode.ALREADY_EXISTS, true, false,
                            "client_id " + clientId + " already has an active connection");
                    return;
                }
                case ConnectionRegistry.RegisterOutcome.Registered registered -> this.connection = registered.connection();
            }

            final long queuedMessageCount;
            try {
                queuedMessageCount = deliveryService.restoreMailboxForConnection(connection);
            } catch (final StorageException e) {
                log.error("mailbox recovery failed for {}", clientId, e);

                // Release the reserved identity so registration can be retried.
                registry.unregister(connection);
                final ClientConnection dead = connection;
                connection = null;

                dead.onClientDisconnected();
                sendProtocolError(ProtocolErrorCode.STORAGE_UNAVAILABLE, true, false,
                        "mailbox recovery failed; retry registration");

                return;
            }

            connection.send(ServerEvent.newBuilder()
                    .setRegistered(Registered.newBuilder()
                            .setClientId(clientId)
                            .setUnackedMessages(queuedMessageCount)
                            .build())
                    .build());

            log.info("client {} registered with {} unacknowledged message(s)", clientId, queuedMessageCount);
            // Only now may deliveries flow.
            deliveryService.startDelivering(connection);
        }

        private void handleSend(final Send send) {
            if (connection == null) {
                rejectUnregistered();
                return;
            }
            final DeliveryService.SendResult result = deliveryService.onSend(
                    connection.clientId(), send.getMessageId(), send.getRecipientId(), send.getContent());
            final ServerEvent response = switch (result) {
                case DeliveryService.SendResult.Accepted a -> ServerEvent.newBuilder()
                        .setSendAccepted(SendAccepted.newBuilder()
                                .setMessageId(send.getMessageId())
                                .setSeqNo(a.seqNo())
                                .build())
                        .build();
                case DeliveryService.SendResult.Rejected r -> ServerEvent.newBuilder()
                        .setSendRejected(SendRejected.newBuilder()
                                .setMessageId(send.getMessageId())
                                .setReason(r.reason())
                                .setRetryable(r.retryable())
                                .setDetail(r.detail())
                                .build())
                        .build();
            };
            connection.send(response);
        }

        private void handleAck(final Ack ack) {
            if (connection == null) {
                rejectUnregistered();
                return;
            }
            final DeliveryService.AckOutcome outcome = deliveryService.onAck(connection, ack.getMessageId());
            if (outcome == DeliveryService.AckOutcome.ACKED) {
                return;
            }
            final boolean retryable = outcome == DeliveryService.AckOutcome.STORAGE_ERROR;
            sendProtocolError(DeliveryService.toProtocolErrorCode(outcome), retryable, false,
                    "ack for " + ack.getMessageId() + " was not applied: " + outcome);
        }

        private void handleUnsetEvent() {
            if (connection == null) {
                rejectUnregistered();
                return;
            }
            sendProtocolError(ProtocolErrorCode.INVALID_EVENT, false, false,
                    "the event oneof was not set");
        }

        private void rejectUnregistered() {
            sendProtocolError(
                    ProtocolErrorCode.REGISTER_REQUIRED,
                    true,
                    false,
                    "register before sending messages or acknowledgements, then retry on this stream"
            );
        }

        @Override
        public void onError(final Throwable t) {
            // Client disconnects are ordinary, so this is info rather than an error.
            if (connection != null) {
                log.info("stream for {} failed: {}", connection.clientId(), t.getMessage());
                deliveryService.onDisconnect(connection);
                connection = null;
            }
        }

        @Override
        public void onCompleted() {
            if (connection != null) {
                connection.complete();
                deliveryService.onDisconnect(connection);
                connection = null;
            } else {
                try {
                    responseObserver.onCompleted();
                } catch (final RuntimeException ignored) {
                    // Already closed.
                }
            }
        }

        private void sendProtocolError(final ProtocolErrorCode code, final boolean retryable, final boolean fatal, final String detail) {
            log.warn("Protocol validation rejected request with code={}, retryable={}, fatal={}",
                    code, retryable, fatal);
            final ServerEvent event = ServerEvent.newBuilder()
                    .setProtocolError(ProtocolError.newBuilder()
                            .setCode(code)
                            .setRetryable(retryable)
                            .setFatal(fatal)
                            .setDetail(detail == null ? "" : detail)
                            .build())
                    .build();
            if (connection != null) {
                connection.send(event);
                return;
            }
            try {
                responseObserver.onNext(event);
            } catch (final RuntimeException ignored) {
                // The stream is gone; the closing status is enough.
            }
        }

        private void closeStream(final Status status) {
            log.info("Closing client stream with status={}", status.getCode());
            final ClientConnection c = connection;
            if (c != null) {
                c.closeWithStatus(status);
                deliveryService.onDisconnect(c);
                connection = null;
                return;
            }
            try {
                responseObserver.onError(status.asRuntimeException());
            } catch (final RuntimeException ignored) {
                // Already closed.
            }
        }
    }
}
