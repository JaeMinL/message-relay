package com.example.relay;

import java.time.Duration;

/**
 * All resource limits and timers in one place. Values can be overridden with
 * -Drelay.<name>=... so tests can shrink timers without touching code.
 */
public final class RelayConfig {

    // --- resource limits ---
    public final int maxMessageContentSize;
    public final int maxClientIdSize;
    public final int maxMessagesPerMailbox;
    public final int maxPendingMessages;
    public final int maxActiveSessions;
    public final int maxOutboundBufferSize;
    public final int executorThreads;
    public final int executorQueueCapacity;

    // --- timers ---
    public final Duration messageTtl;
    public final Duration acknowledgementTimeout;
    public final Duration retryScanInterval;
    public final Duration cleanupInterval;
    public final Duration shutdownGracePeriod;

    // --- transport / storage ---
    public final int port;
    public final int maxInboundMessageSize;
    public final Duration maxConnectionIdle;
    public final Duration maxConnectionAge;
    public final Duration maxConnectionAgeGrace;
    public final Duration keepAliveTimeout;
    public final String jdbcUrl;

    private RelayConfig(final Builder b) {
        this.maxMessageContentSize = b.maxMessageContentSize;
        this.maxClientIdSize = b.maxClientIdSize;
        this.maxMessagesPerMailbox = b.maxMessagesPerMailbox;
        this.maxPendingMessages = b.maxPendingMessages;
        this.maxActiveSessions = b.maxActiveSessions;
        this.maxOutboundBufferSize = b.maxOutboundBufferSize;
        this.executorThreads = b.executorThreads;
        this.executorQueueCapacity = b.executorQueueCapacity;
        this.messageTtl = b.messageTtl;
        this.acknowledgementTimeout = b.acknowledgementTimeout;
        this.retryScanInterval = b.retryScanInterval;
        this.cleanupInterval = b.cleanupInterval;
        this.shutdownGracePeriod = b.shutdownGracePeriod;
        this.port = b.port;
        this.maxInboundMessageSize = b.maxInboundMessageSize;
        this.maxConnectionIdle = b.maxConnectionIdle;
        this.maxConnectionAge = b.maxConnectionAge;
        this.maxConnectionAgeGrace = b.maxConnectionAgeGrace;
        this.keepAliveTimeout = b.keepAliveTimeout;
        this.jdbcUrl = b.jdbcUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Defaults, with -Drelay.* system property overrides applied. */
    public static RelayConfig fromSystemProperties() {
        final Builder b = builder();
        b.port = intProp("relay.port", b.port);
        b.executorThreads = intProp("relay.executorThreads", b.executorThreads);
        b.maxActiveSessions = intProp("relay.maxActiveSessions", b.maxActiveSessions);
        b.maxMessagesPerMailbox = intProp("relay.maxMessagesPerMailbox", b.maxMessagesPerMailbox);
        b.maxPendingMessages = intProp("relay.maxPendingMessages", b.maxPendingMessages);
        final String url = System.getProperty("relay.jdbcUrl");
        if (url != null && !url.isBlank()) {
            b.jdbcUrl = url;
        } else {
            final String dir = System.getProperty("relay.dataDir", "./data");
            b.jdbcUrl = "jdbc:h2:file:" + dir + "/relay;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1";
        }
        return b.build();
    }

    private static int intProp(final String key, final int fallback) {
        final String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("invalid int for " + key + ": " + v, e);
        }
    }

    public static final class Builder {
        private int maxMessageContentSize = 64 * 1024;
        private int maxClientIdSize = 128;
        private int maxMessagesPerMailbox = 1_000;
        private int maxPendingMessages = 100_000;
        private int maxActiveSessions = 1_000;
        private int maxOutboundBufferSize = 32;
        // Delivery work is I/O bound (JDBC + stream write), so ~2x cores.
        private int executorThreads = Math.max(4, Math.min(16, Runtime.getRuntime().availableProcessors() * 2));
        private int executorQueueCapacity = 1_000;

        private Duration messageTtl = Duration.ofHours(24);
        private Duration acknowledgementTimeout = Duration.ofSeconds(30);
        private Duration retryScanInterval = Duration.ofSeconds(5);
        private Duration cleanupInterval = Duration.ofMinutes(5);
        private Duration shutdownGracePeriod = Duration.ofSeconds(30);

        private int port = 50051;
        private int maxInboundMessageSize = 128 * 1024;
        private Duration maxConnectionIdle = Duration.ofHours(24);
        private Duration maxConnectionAge = Duration.ofHours(24);
        private Duration maxConnectionAgeGrace = Duration.ofSeconds(30);
        private Duration keepAliveTimeout = Duration.ofSeconds(20);
        private String jdbcUrl = "jdbc:h2:file:./data/relay;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1";

        /** Sets the UTF-8 byte limit for a message body. */
        public Builder maxMessageContentSize(final int value) {
            this.maxMessageContentSize = value;
            return this;
        }

        /** Sets the maximum unexpired messages retained for one recipient. */
        public Builder maxMessagesPerMailbox(final int value) {
            this.maxMessagesPerMailbox = value;
            return this;
        }

        /** Sets the maximum number of registered client connections. */
        public Builder maxActiveSessions(final int value) {
            this.maxActiveSessions = value;
            return this;
        }

        /** Sets how long a stored message remains eligible for delivery. */
        public Builder messageTtl(final Duration value) {
            this.messageTtl = value;
            return this;
        }

        /** Sets how long delivery waits for an acknowledgement before retrying. */
        public Builder acknowledgementTimeout(final Duration value) {
            this.acknowledgementTimeout = value;
            return this;
        }

        /** Sets the interval between retry and delivery recovery scans. */
        public Builder retryScanInterval(final Duration value) {
            this.retryScanInterval = value;
            return this;
        }

        /** Sets the interval between expired-message cleanup sweeps. */
        public Builder cleanupInterval(final Duration value) {
            this.cleanupInterval = value;
            return this;
        }

        /** Sets the JDBC URL used by the mailbox store. */
        public Builder jdbcUrl(final String value) {
            this.jdbcUrl = value;
            return this;
        }

        public RelayConfig build() { return new RelayConfig(this); }
    }
}
