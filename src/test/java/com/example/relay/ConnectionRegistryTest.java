package com.example.relay;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ConnectionRegistryTest {
    @Test
    void duplicateLiveIdPreservesTheOriginalConnection() {
        // given: a live client already owns the only registry slot
        final ConnectionRegistry registry = new ConnectionRegistry(1);
        final ClientConnection original = connection("bob");
        registry.register(original);
        final ClientConnection duplicate = connection("bob");
        // when: another connection claims the same ID
        final var result = registry.register(duplicate);
        // then: duplicate-ID rejection takes precedence over capacity and preserves ownership
        assertInstanceOf(ConnectionRegistry.RegisterOutcome.DuplicateId.class, result);
        assertSame(original, registry.findActiveConnection("bob"));
        assertEquals(1, registry.registeredConnections().size());
        assertFalse(original.isClosed());
        assertFalse(duplicate.isClosed());
    }

    @Test
    void unregisteringARejectedDuplicateCannotRemoveTheOwner() {
        // given: a second connection was rejected for using a live ID
        final ConnectionRegistry registry = new ConnectionRegistry(2);
        final ClientConnection original = connection("bob");
        final ClientConnection rejected = connection("bob");
        registry.register(original);
        assertInstanceOf(ConnectionRegistry.RegisterOutcome.DuplicateId.class, registry.register(rejected));
        // when: the rejected connection is cleaned up
        registry.unregister(rejected);
        // then: identity-based removal leaves the original owner in place
        assertSame(original, registry.findActiveConnection("bob"));
        assertEquals(1, registry.registeredConnections().size());
    }

    @Test
    void replacingAClosedConnectionDoesNotConsumeAnotherSlot() {
        // given: a closed connection still occupying the sole registry slot
        final ConnectionRegistry registry = new ConnectionRegistry(1);
        final ClientConnection previous = connection("bob");
        registry.register(previous);
        previous.onClientDisconnected();
        final ClientConnection replacement = connection("bob");
        // when: a replacement claims the ID and the old connection unregisters late
        final var result = registry.register(replacement);
        registry.unregister(previous);
        // then: the new connection remains registered without exceeding capacity
        assertInstanceOf(ConnectionRegistry.RegisterOutcome.Registered.class, result);
        assertSame(replacement, registry.findActiveConnection("bob"));
        assertEquals(1, registry.registeredConnections().size());
        assertInstanceOf(ConnectionRegistry.RegisterOutcome.LimitReached.class, registry.register(connection("alice")));
    }

    private static ClientConnection connection(final String clientId) {
        return new ClientConnection(clientId, mock(StreamObserver.class), 1);
    }
}
