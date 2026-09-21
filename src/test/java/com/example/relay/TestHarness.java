package com.example.relay;

import com.example.relay.proto.ClientEvent;
import com.example.relay.proto.MessageRelayGrpc;
import com.example.relay.proto.ServerEvent;
import com.example.relay.store.MailboxStore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * In-process server plus a minimal client, so tests exercise the real gRPC
 * plumbing without binding a port. Every wait is bounded by a timeout so a hang
 * fails the test instead of the suite.
 */
final class TestHarness implements AutoCloseable {

    static final Duration AWAIT = Duration.ofSeconds(5);

    private final String serverName = InProcessServerBuilder.generateName();
    private final RelayConfig config;
    private final MailboxStore store;
    private final ConnectionRegistry registry;
    private final DeliveryService deliveryService;
    private final Server server;
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final ExecutorService callExecutor =
            Executors.newCachedThreadPool(Thread.ofPlatform().name("test-grpc-", 0).factory());

    TestHarness(RelayConfig.Builder builder) throws IOException {
        // A private in-memory database per harness keeps tests independent.
        this.config = builder
                .jdbcUrl("jdbc:h2:mem:relay-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1")
                .build();
        this.store = new MailboxStore(config);
        this.registry = new ConnectionRegistry(config.maxActiveSessions);
        this.deliveryService = new DeliveryService(config, store, registry);
        // Deliberately NOT directExecutor(): with a bidirectional stream a direct
        // executor runs the server handler on the same thread that is delivering an
        // event to the client, so an inbound ACK can block on the write lock a
        // delivery still holds. Real transports never do that; a normal executor
        // keeps the test faithful to production behaviour.
        this.server = InProcessServerBuilder.forName(serverName)
                .executor(callExecutor)
                .addService(new RelayServiceImpl(config, registry, deliveryService))
                .build();
        deliveryService.start();
        server.start();
    }

    static TestHarness withDefaults() throws IOException {
        return new TestHarness(RelayConfig.builder());
    }

    MailboxStore store() {
        return store;
    }

    DeliveryService deliveryService() {
        return deliveryService;
    }

    ConnectionRegistry registry() {
        return registry;
    }

    RelayConfig config() {
        return config;
    }

    /** Opens a stream without registering, for protocol-order tests. */
    TestClient openRaw() {
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName)
                .executor(callExecutor)
                .build();
        channels.add(channel);
        return new TestClient(channel);
    }

    /** Opens a stream and waits for the Registered acknowledgement. */
    TestClient connect(String clientId) throws InterruptedException {
        TestClient client = openRaw();
        client.register(clientId);
        assertNotNull(client.awaitRegistered(), "expected a Registered event for " + clientId);
        return client;
    }

    @Override
    public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
        server.shutdownNow();
        deliveryService.close();
        store.close();
        callExecutor.shutdownNow();
    }

    /** Collects every ServerEvent into a queue the tests poll with a timeout. */
    static final class TestClient implements AutoCloseable {
        private final BlockingQueue<ServerEvent> events = new LinkedBlockingQueue<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch finished = new CountDownLatch(1);
        private final StreamObserver<ClientEvent> outbound;

        TestClient(ManagedChannel channel) {
            this.outbound = MessageRelayGrpc.newStub(channel).connect(new StreamObserver<>() {
                @Override
                public void onNext(ServerEvent value) {
                    events.add(value);
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                    finished.countDown();
                }

                @Override
                public void onCompleted() {
                    finished.countDown();
                }
            });
        }

        void send(ClientEvent event) {
            outbound.onNext(event);
        }

        void register(String clientId) {
            send(ClientEvent.newBuilder()
                    .setRegister(com.example.relay.proto.Register.newBuilder().setClientId(clientId))
                    .build());
        }

        void sendMessage(String messageId, String recipientId, String content) {
            send(ClientEvent.newBuilder()
                    .setSend(com.example.relay.proto.Send.newBuilder()
                            .setMessageId(messageId)
                            .setRecipientId(recipientId)
                            .setContent(content))
                    .build());
        }

        void ack(String messageId) {
            send(ClientEvent.newBuilder()
                    .setAck(com.example.relay.proto.Ack.newBuilder().setMessageId(messageId))
                    .build());
        }

        /** Next event of any kind, or null on timeout. */
        ServerEvent poll() throws InterruptedException {
            return events.poll(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        }

        /** Next event matching a case, skipping others. Null on timeout. */
        ServerEvent pollFor(ServerEvent.EventCase wanted) throws InterruptedException {
            long deadline = System.nanoTime() + AWAIT.toNanos();
            while (System.nanoTime() < deadline) {
                ServerEvent event = events.poll(200, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                if (event.getEventCase() == wanted) {
                    return event;
                }
            }
            return null;
        }

        com.example.relay.proto.Registered awaitRegistered() throws InterruptedException {
            ServerEvent event = pollFor(ServerEvent.EventCase.REGISTERED);
            return event == null ? null : event.getRegistered();
        }

        com.example.relay.proto.Delivery awaitDelivery() throws InterruptedException {
            ServerEvent event = pollFor(ServerEvent.EventCase.DELIVERY);
            return event == null ? null : event.getDelivery();
        }

        /** Waits for the stream to terminate. Returns false on timeout. */
        boolean awaitFinished() throws InterruptedException {
            return finished.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS);
        }

        Throwable error() {
            return error.get();
        }

        /** Simulates an abrupt client disconnect. */
        void abort() {
            outbound.onError(new RuntimeException("client aborted"));
        }

        @Override
        public void close() {
            try {
                outbound.onCompleted();
            } catch (RuntimeException ignored) {
                // Already closed.
            }
        }
    }
}
