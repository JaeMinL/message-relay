package com.example.relay;

import com.example.relay.store.MailboxStore;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RelayServerTest {
    @Test
    void mainBuildsConfiguredTransportAndStartsDeliveryBeforeServing() throws Exception {
        // given: controlled transport and storage factories without opening a port or file database
        final RelayConfig config = RelayConfig.builder().build();
        final Server server = mock(Server.class);
        final NettyServerBuilder builder = mock(NettyServerBuilder.class, RETURNS_SELF);
        when(builder.build()).thenReturn(server);
        when(server.start()).thenReturn(server);
        try (final var configuration = mockStatic(RelayConfig.class);
             final var transport = mockStatic(NettyServerBuilder.class);
             final var stores = mockConstruction(MailboxStore.class);
             final var deliveries = mockConstruction(DeliveryService.class)) {
            configuration.when(RelayConfig::fromSystemProperties).thenReturn(config);
            transport.when(() -> NettyServerBuilder.forPort(config.port)).thenReturn(builder);
            // when: the application entry point starts and waits for transport termination
            RelayServer.main(new String[0]);
            // then: configured limits are applied and serving starts after delivery timers
            verify(builder).maxInboundMessageSize(config.maxInboundMessageSize);
            verify(builder).maxConnectionIdle(config.maxConnectionIdle.toSeconds(), TimeUnit.SECONDS);
            verify(builder).maxConnectionAge(config.maxConnectionAge.toSeconds(), TimeUnit.SECONDS);
            verify(builder).maxConnectionAgeGrace(config.maxConnectionAgeGrace.toSeconds(), TimeUnit.SECONDS);
            verify(builder).keepAliveTimeout(config.keepAliveTimeout.toSeconds(), TimeUnit.SECONDS);
            verify(builder).permitKeepAliveWithoutCalls(true);
            final var order = inOrder(deliveries.constructed().get(0), server);
            order.verify(deliveries.constructed().get(0)).start();
            order.verify(server).start();
            order.verify(server).awaitTermination();
            assertEquals(1, stores.constructed().size());
        }
    }

    @Test
    void shutdownClosesOwnedResourcesOnSuccessTimeoutAndInterruption() throws Exception {
        // given: transport termination can succeed, time out or be interrupted
        for (final String outcome : new String[]{"success", "timeout", "interrupted"}) {
            final Server server = mock(Server.class);
            final MailboxStore store = mock(MailboxStore.class);
            final DeliveryService delivery = mock(DeliveryService.class);
            if (outcome.equals("interrupted")) {
                when(server.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
            } else {
                when(server.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(outcome.equals("success"));
            }
            final var constructor = RelayServer.class.getDeclaredConstructor(RelayConfig.class, MailboxStore.class,
                    ConnectionRegistry.class, DeliveryService.class, Server.class);
            constructor.setAccessible(true);
            final RelayServer relay = constructor.newInstance(RelayConfig.builder().build(), store,
                    new ConnectionRegistry(1), delivery, server);
            try {
                // when: the server shuts down
                relay.close();
                // then: transport closes before workers and storage, with forced termination if needed
                final var order = inOrder(server, delivery, store);
                order.verify(server).shutdown();
                order.verify(delivery).close();
                order.verify(store).close();
                verify(server, times(outcome.equals("success") ? 0 : 1)).shutdownNow();
                assertEquals(outcome.equals("interrupted"), Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
    }
}
