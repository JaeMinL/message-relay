package com.example.relay;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Active client streams, keyed by client id. One connection per id: a second
 * stream claiming a live id is rejected rather than displacing the first.
 */
public final class ConnectionRegistry {

    private final Map<String, ClientConnection> activeConnections = new ConcurrentHashMap<>();
    private final int maxActiveSessions;

    public ConnectionRegistry(final int maxActiveSessions) {
        this.maxActiveSessions = maxActiveSessions;
    }

    public sealed interface RegisterOutcome {
        record Registered(ClientConnection connection) implements RegisterOutcome {}
        record DuplicateId() implements RegisterOutcome {}
        record LimitReached() implements RegisterOutcome {}
    }

    /**
     * Claims a client ID while holding the registry lock so capacity checks and
     * insertion are atomic with respect to other registrations and removals.
     */
    public synchronized RegisterOutcome register(final ClientConnection newConnection) {
        final String clientId = newConnection.clientId();
        final ClientConnection existing = activeConnections.get(clientId);

        if (existing != null && !existing.isClosed()) {
            return new RegisterOutcome.DuplicateId();
        }

        if (existing != null) {
            activeConnections.put(clientId, newConnection);
            return new RegisterOutcome.Registered(newConnection);
        }

        if (activeConnections.size() >= maxActiveSessions) {
            return new RegisterOutcome.LimitReached();
        }

        activeConnections.put(clientId, newConnection);
        return new RegisterOutcome.Registered(newConnection);
    }

    /** Removes the entry only if it still points at this connection. */
    public synchronized void unregister(final ClientConnection connection) {
        activeConnections.remove(connection.clientId(), connection);
    }

    public ClientConnection findActiveConnection(final String clientId) {
        final ClientConnection connection = activeConnections.get(clientId);
        return connection == null || connection.isClosed() ? null : connection;
    }

    public Collection<ClientConnection> registeredConnections() {
        return activeConnections.values();
    }
}
