package com.example.relay;

import com.example.relay.proto.*;
import com.example.relay.store.StorageException;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RelayServiceImplTest {
    private final RelayConfig config = RelayConfig.builder().build();
    private final ConnectionRegistry registry = new ConnectionRegistry(config.maxActiveSessions);
    private final DeliveryService delivery = mock(DeliveryService.class);
    private final RelayServiceImpl service = new RelayServiceImpl(config, registry, delivery);

    @Test
    void repeatedRegistrationOnTheSameStreamIsNonfatalAndDoesNotRestoreTwice() {
        // given: a successfully registered stream
        final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        final var stream = service.connect(observer);
        stream.onNext(register());
        final ClientConnection original = registry.findActiveConnection("bob");
        // when: the same registration is repeated
        stream.onNext(register());
        // then: the original identity remains and initialization is not repeated
        final var events = ArgumentCaptor.forClass(ServerEvent.class);
        verify(observer, times(2)).onNext(events.capture());
        final ProtocolError error = events.getAllValues().get(1).getProtocolError();
        assertEquals(ProtocolErrorCode.ALREADY_REGISTERED, error.getCode());
        assertFalse(error.getFatal());
        assertFalse(error.getRetryable());
        assertSame(original, registry.findActiveConnection("bob"));
        verify(delivery).restoreMailboxForConnection(original);
        verify(delivery).startDelivering(original);
        verify(observer, never()).onError(any());
        verify(observer, never()).onCompleted();
    }

    @Test
    void registeredStreamCannotSwitchClientIdentityByRegisteringAgain() {
        // given: a stream registered as bob
        final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        final var stream = service.connect(observer);
        stream.onNext(register());
        final ClientConnection original = registry.findActiveConnection("bob");
        // when: the same stream attempts to become carol
        stream.onNext(ClientEvent.newBuilder().setRegister(Register.newBuilder().setClientId("carol")).build());
        // then: the request is rejected without granting another identity or disconnecting bob
        verify(observer).onNext(argThat(event -> event.hasProtocolError()
                && event.getProtocolError().getCode() == ProtocolErrorCode.ALREADY_REGISTERED
                && !event.getProtocolError().getFatal()));
        assertSame(original, registry.findActiveConnection("bob"));
        assertNull(registry.findActiveConnection("carol"));
        verify(delivery, never()).isValidClientId("carol");
        verify(delivery, never()).onDisconnect(any());
        assertFalse(original.isClosed());
    }

    @Test
    void secondStreamWithDuplicateIdCanCorrectItsIdWithoutDisplacingTheOwner() {
        // given: bob has an established stream and a new stream has no identity
        final StreamObserver<ServerEvent> originalObserver = mock(StreamObserver.class);
        final StreamObserver<ServerEvent> newObserver = mock(StreamObserver.class);
        when(delivery.isValidClientId(anyString())).thenReturn(true);
        service.connect(originalObserver).onNext(register());
        final ClientConnection original = registry.findActiveConnection("bob");
        final var newStream = service.connect(newObserver);
        // when: the new stream claims bob's identity
        newStream.onNext(register());
        // then: rejection is recoverable and does not restore or replace bob's mailbox
        verify(newObserver).onNext(argThat(event -> event.hasProtocolError()
                && event.getProtocolError().getCode() == ProtocolErrorCode.ALREADY_EXISTS
                && event.getProtocolError().getRetryable() && !event.getProtocolError().getFatal()));
        assertSame(original, registry.findActiveConnection("bob"));
        assertEquals(1, registry.registeredConnections().size());
        verify(delivery, times(1)).restoreMailboxForConnection(any());
        verify(originalObserver, never()).onError(any());
        verify(originalObserver, never()).onCompleted();

        // when: the rejected stream corrects its requested identity
        newStream.onNext(ClientEvent.newBuilder().setRegister(Register.newBuilder().setClientId("carol")).build());
        // then: the same stream is registered successfully and bob remains connected
        verify(newObserver).onNext(argThat(event -> event.hasRegistered()
                && event.getRegistered().getClientId().equals("carol")));
        assertSame(original, registry.findActiveConnection("bob"));
        assertNotNull(registry.findActiveConnection("carol"));
        verify(newObserver, never()).onError(any());
        verify(newObserver, never()).onCompleted();
    }

    @Test
    void failedMailboxRestoreReleasesTheIdAndAllowsRegistrationRetry() {
        // given: a transient storage failure during the first registration
        final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        when(delivery.restoreMailboxForConnection(any())).thenThrow(new StorageException("offline", null)).thenReturn(2L);
        final var stream = service.connect(observer);
        // when: registration is retried on the same stream
        stream.onNext(register());
        assertNull(registry.findActiveConnection("bob"));
        stream.onNext(register());
        // then: the first response is recoverable and the second confirms registration
        final var events = ArgumentCaptor.forClass(ServerEvent.class);
        verify(observer, times(2)).onNext(events.capture());
        assertEquals(ProtocolErrorCode.STORAGE_UNAVAILABLE, events.getAllValues().get(0).getProtocolError().getCode());
        assertFalse(events.getAllValues().get(0).getProtocolError().getFatal());
        assertTrue(events.getAllValues().get(0).getProtocolError().getRetryable());
        assertEquals(2, events.getAllValues().get(1).getRegistered().getUnackedMessages());
        verify(observer, never()).onError(any());
        verify(delivery).startDelivering(registry.findActiveConnection("bob"));
    }

    @Test
    void readyCallbacksOnlyDeliverForAnOpenRegisteredConnection() {
        // given: transport readiness callbacks and a registered client
        final ServerCallStreamObserver<ServerEvent> observer = mock(ServerCallStreamObserver.class);
        when(observer.isReady()).thenReturn(true);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        final var stream = service.connect(observer);
        final var callback = ArgumentCaptor.forClass(Runnable.class);
        verify(observer).setOnReadyHandler(callback.capture());
        // when: readiness fires before registration, after registration and after closure
        callback.getValue().run();
        stream.onNext(register());
        final ClientConnection connection = registry.findActiveConnection("bob");
        callback.getValue().run();
        connection.onClientDisconnected();
        callback.getValue().run();
        // then: only the open registered connection is offered delivery work
        verify(delivery).deliverNextMessageIfReady(connection);
    }

    @Test
    void unsetEventsAndAckStorageErrorsKeepRegisteredStreamsOpen() {
        // given: an established stream and unavailable ACK storage
        final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        when(delivery.onAck(any(), anyString())).thenReturn(DeliveryService.AckOutcome.STORAGE_ERROR);
        final var stream = service.connect(observer);
        stream.onNext(register());
        // when: empty events and failing acknowledgements arrive
        stream.onNext(ClientEvent.getDefaultInstance());
        stream.onNext(ClientEvent.newBuilder().setAck(Ack.newBuilder().setMessageId("message")).build());
        // then: both errors are nonfatal and the storage error is retryable
        final var events = ArgumentCaptor.forClass(ServerEvent.class);
        verify(observer, times(3)).onNext(events.capture());
        assertEquals(ProtocolErrorCode.INVALID_EVENT, events.getAllValues().get(1).getProtocolError().getCode());
        assertEquals(ProtocolErrorCode.STORAGE_UNAVAILABLE, events.getAllValues().get(2).getProtocolError().getCode());
        assertTrue(events.getAllValues().get(2).getProtocolError().getRetryable());
        verify(observer, never()).onError(any());
    }

    @Test
    void unexpectedFailureBeforeRegistrationTerminatesTheStream() {
        // given: registration validation fails unexpectedly and transport teardown may throw
        for (final boolean teardownThrows : new boolean[]{false, true}) {
            final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
            if (teardownThrows) doThrow(new IllegalStateException("gone")).when(observer).onError(any());
            doThrow(new IllegalStateException("unexpected")).when(delivery).isValidClientId("bob");
            final var stream = service.connect(observer);
            // when: the request handler catches an unexpected error
            assertDoesNotThrow(() -> stream.onNext(register()));
            // then: the terminal status is INTERNAL even if writing it fails
            verify(observer).onError(argThat(error -> Status.fromThrowable(error).getCode() == Status.Code.INTERNAL));
        }
    }

    @Test
    void overflowAfterRegistrationTerminatesWithResourceExhausted() {
        // given: an established stream whose next send processing overflows
        final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
        when(delivery.isValidClientId("bob")).thenReturn(true);
        when(delivery.onSend(anyString(), anyString(), anyString(), anyString()))
                .thenThrow(new ClientConnection.OutboundOverflowException("bob"));
        final var stream = service.connect(observer);
        stream.onNext(register());
        final ClientConnection connection = registry.findActiveConnection("bob");
        // when: send processing signals exhausted outbound capacity
        stream.onNext(ClientEvent.newBuilder().setSend(Send.getDefaultInstance()).build());
        // then: the observer receives a terminal status and disconnect cleanup runs
        verify(observer).onError(argThat(error -> Status.fromThrowable(error).getCode() == Status.Code.RESOURCE_EXHAUSTED));
        verify(delivery).onDisconnect(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    void rawStreamCompletionAndFailedErrorWritesAreContained() {
        // given: unregistered streams, including an already torn-down observer
        for (final boolean teardownThrows : new boolean[]{false, true}) {
            final StreamObserver<ServerEvent> observer = mock(StreamObserver.class);
            if (teardownThrows) {
                doThrow(new IllegalStateException("gone")).when(observer).onNext(any());
                doThrow(new IllegalStateException("gone")).when(observer).onCompleted();
            }
            final var stream = service.connect(observer);
            // when: a protocol error is emitted, followed by completion
            assertDoesNotThrow(() -> stream.onNext(ClientEvent.getDefaultInstance()));
            assertDoesNotThrow(stream::onCompleted);
            stream.onError(new IllegalStateException("already gone"));
            // then: callbacks were attempted without starting delivery or disconnecting an absent client
            verify(observer).onNext(any());
            verify(observer).onCompleted();
            verify(delivery, never()).onDisconnect(any());
        }
    }

    private static ClientEvent register() {
        return ClientEvent.newBuilder().setRegister(Register.newBuilder().setClientId("bob")).build();
    }
}
