package com.example.relay;

import com.example.relay.store.MailboxStore;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Process entry point and lifecycle owner.
 *
 * <p>Transport limits are set in code rather than in a config file so there is no
 * gap between what is written down and what the server actually enforces.
 */
public final class RelayServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RelayServer.class);

    private final RelayConfig config;
    private final MailboxStore store;
    private final ConnectionRegistry registry;
    private final DeliveryService deliveryService;
    private final Server server;

    /**
     * Builds a server on a TCP port with the documented transport limits applied.
     */
    private static RelayServer forPort(final RelayConfig config)
    {
        final MailboxStore store = new MailboxStore(config);
        final ConnectionRegistry registry = new ConnectionRegistry(config.maxActiveSessions);
        final DeliveryService delivery = new DeliveryService(config, store, registry);
        final Server server = NettyServerBuilder.forPort(config.port)
                .addService(new RelayServiceImpl(config, registry, delivery))
                .maxInboundMessageSize(config.maxInboundMessageSize)
                .maxConnectionIdle(config.maxConnectionIdle.toSeconds(), TimeUnit.SECONDS)
                .maxConnectionAge(config.maxConnectionAge.toSeconds(), TimeUnit.SECONDS)
                .maxConnectionAgeGrace(config.maxConnectionAgeGrace.toSeconds(), TimeUnit.SECONDS)
                .keepAliveTimeout(config.keepAliveTimeout.toSeconds(), TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .build();
        return new RelayServer(config, store, registry, delivery, server);
    }

    private RelayServer(final RelayConfig config,
                        final MailboxStore store,
                        final ConnectionRegistry registry,
                        final DeliveryService deliveryService,
                        final Server server)
    {
        this.config = config;
        this.store = store;
        this.registry = registry;
        this.deliveryService = deliveryService;
        this.server = server;
    }

    public void start() throws IOException {
        deliveryService.start();
        server.start();
        log.info("relay listening on port {} (storage: {})", server.getPort(), config.jdbcUrl);
    }


    private void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /**
     * Stops accepting work, lets in-flight delivery tasks finish, then releases
     * storage. Ordering matters: the transport goes first so no new events arrive
     * while the executor drains.
     */
    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(config.shutdownGracePeriod.toSeconds(), TimeUnit.SECONDS)) {
                log.warn("forcing transport shutdown after the grace period");
                server.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
        deliveryService.close();
        store.close();
        log.info("relay stopped");
    }

    public static void main(final String[] args) throws Exception {
        final RelayConfig config = RelayConfig.fromSystemProperties();
        final RelayServer relay = RelayServer.forPort(config);
        Runtime.getRuntime().addShutdownHook(new Thread(relay::close, "relay-shutdown"));
        relay.start();
        relay.awaitTermination();
    }
}
