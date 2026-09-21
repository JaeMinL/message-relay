package com.example.relay;

import com.example.relay.proto.ProtocolErrorCode;
import com.example.relay.proto.SendRejectionReason;
import com.example.relay.proto.ServerEvent;
import com.example.relay.store.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DeliveryServiceTest {

@Test
    void recipientRequeueOnlyResetsThatRecipientsInflightMessages() {
        // given: queued and in-flight messages belonging to different recipients
        try (final StoreFixture f = new StoreFixture()) {
            final String firstId = f.enqueue("first");
            final String queuedId = f.enqueue("queued");
            final long firstSeqNo = f.store.findByMessageId(firstId).seqNo();
            final String otherId = UUID.randomUUID().toString();
            final long otherSeqNo = f.store.enqueue(otherId, "alice", "other", "other", System.currentTimeMillis());
            assertTrue(f.store.markInFlight(firstSeqNo, System.currentTimeMillis()));
            assertTrue(f.store.markInFlight(otherSeqNo, System.currentTimeMillis()));

            // when: only bob's in-flight messages are requeued
            final int requeued = f.store.requeueInFlightMessagesForRecipient("bob");
            // then: bob's pending state is reset without changing the other recipient
            assertEquals(1, requeued);
            assertEquals(MessageStatus.QUEUED, f.store.findByMessageId(firstId).status());
            assertNull(f.store.findByMessageId(firstId).deliveredAtMillis());
            assertEquals(MessageStatus.QUEUED, f.store.findByMessageId(queuedId).status());
            assertEquals(MessageStatus.IN_FLIGHT, f.store.findByMessageId(otherId).status());
            assertEquals(0, f.store.requeueInFlightMessagesForRecipient("missing"));
        }
    }

    @Test
    void purgedInflightMessageDoesNotBlockNextDelivery() {
        // given: a delivered message is purged while the connection is awaiting its ACK
        try (StoreFixture f = new StoreFixture()) {
            String firstId = f.enqueue("first");
            f.service.deliverNextMessageIfReady(f.connection);
            assertEquals(1, f.received.size());

            // Advance the cleanup cutoff without sleeping or starting timers.
            assertEquals(1, f.store.purgeExpired(
                    System.currentTimeMillis() + f.config.messageTtl.toMillis() + 1));
            assertEquals(DeliveryService.AckOutcome.UNKNOWN,  f.service.onAck(f.connection, firstId));
            String secondId = f.enqueue("second");
            // when: retry scanning and delivery run after the stale row has disappeared
            f.service.requeueStaleSafely();
            f.service.deliverNextMessageIfReady(f.connection);

            // then: the next message is delivered as a first attempt
            assertEquals(2, f.received.size());
            assertEquals(secondId, f.received.get(1).getDelivery().getMessageId());
            assertFalse(f.received.get(1).getDelivery().getRedelivery());
        }
    }

    @Test
    void validInflightMessageStillBlocksNextDelivery() {
        // given: a valid message awaiting ACK and another message behind it
        try (StoreFixture f = new StoreFixture()) {
            f.enqueue("first");
            f.service.deliverNextMessageIfReady(f.connection);
            String secondId = f.enqueue("second");
            // when: delivery is attempted repeatedly before ACK
            f.service.deliverNextMessageIfReady(f.connection);
            f.service.deliverNextMessageIfReady(f.connection);

            // then: no additional message is sent and the second remains queued
            assertEquals(1, f.received.size());
            assertEquals(MessageStatus.QUEUED, f.store.findByMessageId(secondId).status());
        }
    }

    private static final class StoreFixture implements AutoCloseable {
        final RelayConfig config = RelayConfig.builder()
                .jdbcUrl("jdbc:h2:mem:delivery-test-" + UUID.randomUUID()).build();
        final MailboxStore store = new MailboxStore(config);
        final DeliveryService service = new DeliveryService(config, store,
                new ConnectionRegistry(config.maxActiveSessions));
        final ArrayList<ServerEvent> received = new ArrayList<>();
        final ClientConnection connection = new ClientConnection("bob", new StreamObserver<>() {
            public void onNext(ServerEvent event) { received.add(event); }
            public void onError(Throwable error) { fail(error); }
            public void onCompleted() { }
        }, config.maxOutboundBufferSize);

        String enqueue(String content) {
            String messageId = UUID.randomUUID().toString();
            store.enqueue(messageId, "alice", "bob", content, System.currentTimeMillis());
            return messageId;
        }

        @Override
        public void close() {
            service.close();
            store.close();
        }
    }

private static final String MESSAGE_ID = UUID.randomUUID().toString();

    @Test
    void sendValidationAndStorageFailuresBecomeRejections() {
        // given: a service whose store may reject admission or fail
        try (final FailureFixture fixture = new FailureFixture()) {
            // when: malformed and missing values are submitted
            assertRejected(fixture.service.onSend("alice", null, "bob", "x"), SendRejectionReason.INVALID_MESSAGE, false);
            assertRejected(fixture.service.onSend("alice", "z".repeat(36), "bob", "x"), SendRejectionReason.INVALID_MESSAGE, false);
            assertRejected(fixture.service.onSend("alice", MESSAGE_ID, null, "x"), SendRejectionReason.INVALID_MESSAGE, false);
            assertRejected(fixture.service.onSend("alice", MESSAGE_ID, "bob", null), SendRejectionReason.INVALID_MESSAGE, false);
            // then: no invalid request reaches storage, while storage failures retain retryability
            verifyNoInteractions(fixture.store);
            when(fixture.store.enqueue(anyString(), anyString(), anyString(), anyString(), anyLong()))
                    .thenThrow(new MailboxFullException("full"), new StorageException("offline", null));
            assertRejected(fixture.service.onSend("alice", MESSAGE_ID, "bob", "x"), SendRejectionReason.RESOURCE_EXHAUSTED, true);
            assertRejected(fixture.service.onSend("alice", MESSAGE_ID, "bob", "x"), SendRejectionReason.STORAGE_FAILURE, true);
        }
    }

    @Test
    void ackOutcomesDistinguishQueuedDeletedAndUnavailableMessages() {
        // given: queued, concurrently deleted and unavailable database states
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.store.findByMessageId(MESSAGE_ID)).thenReturn(message(MessageStatus.QUEUED), message(MessageStatus.IN_FLIGHT))
                    .thenThrow(new StorageException("offline", null));
            // when: the same ACK is evaluated against each state
            final var queued = fixture.service.onAck(fixture.connection, MESSAGE_ID);
            final var deleted = fixture.service.onAck(fixture.connection, MESSAGE_ID);
            final var unavailable = fixture.service.onAck(fixture.connection, MESSAGE_ID);
            // then: errors map to their protocol codes without propagating storage failures
            assertEquals(DeliveryService.AckOutcome.NOT_IN_FLIGHT, queued);
            assertEquals(DeliveryService.AckOutcome.UNKNOWN, deleted);
            assertEquals(DeliveryService.AckOutcome.STORAGE_ERROR, unavailable);
            assertEquals(ProtocolErrorCode.ACK_NOT_IN_FLIGHT, DeliveryService.toProtocolErrorCode(queued));
            assertEquals(ProtocolErrorCode.STORAGE_UNAVAILABLE, DeliveryService.toProtocolErrorCode(unavailable));
            assertEquals(ProtocolErrorCode.PROTOCOL_ERROR_CODE_UNSPECIFIED,
                    DeliveryService.toProtocolErrorCode(DeliveryService.AckOutcome.ACKED));
        }
    }

    @Test
    void deliverySkipsClosedConnectionsAndLostDatabaseClaims() {
        // given: a connection that closes before delivery
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.connection.isClosed()).thenReturn(true);
            // when: attempting to deliver on the closed connection
            fixture.service.deliverNextMessageIfReady(fixture.connection);
            // then: storage is untouched; a later lost claim must not write either
            verifyNoInteractions(fixture.store);
            when(fixture.connection.isClosed()).thenReturn(false);
            when(fixture.store.loadNext(eq("bob"), anyLong())).thenReturn(message(MessageStatus.QUEUED));
            fixture.service.deliverNextMessageIfReady(fixture.connection);
            verify(fixture.connection, never()).sendLocked(any());
        }
    }

    @Test
    void aBusyConnectionDoesNotBlockTheDeliveryWorker() {
        // given: a write lock already held by another worker
        try (final FailureFixture fixture = new FailureFixture()) {
            final ReentrantLock busyLock = mock(ReentrantLock.class);
            when(fixture.connection.writeLock()).thenReturn(busyLock);
            when(busyLock.tryLock()).thenReturn(false);
            // when: another delivery is attempted
            fixture.service.deliverNextMessageIfReady(fixture.connection);
            // then: it returns without querying or unlocking someone else's lock
            verifyNoInteractions(fixture.store);
            verify(busyLock, never()).unlock();
        }
    }

    @Test
    void failedWritesRequeueMessagesAndAlwaysReleaseTheLock() {
        // given: a successfully claimed message whose outbound write fails
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.store.loadNext(eq("bob"), anyLong())).thenReturn(message(MessageStatus.QUEUED));
            when(fixture.store.markInFlight(eq(1L), anyLong())).thenReturn(true);
            when(fixture.store.requeueInFlightMessage(1L)).thenReturn(true);
            // when: sendLocked returns false
            fixture.service.deliverNextMessageIfReady(fixture.connection);
            // then: the message is requeued and the connection slot is not reserved
            verify(fixture.store).requeueInFlightMessage(1L);
            verify(fixture.connection, never()).setInFlightSeqNo(1L);
            assertFalse(fixture.lock.isLocked());
        }
    }

    @Test
    void deliveryStorageFailuresAreLoggedAndOverflowIsPropagatedToTheCaller() {
        // given: a failed query followed by a valid message and outbound overflow
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.store.loadNext(eq("bob"), anyLong())).thenThrow(new StorageException("offline", null))
                    .thenReturn(message(MessageStatus.QUEUED));
            when(fixture.store.markInFlight(eq(1L), anyLong())).thenReturn(true);
            when(fixture.connection.sendLocked(any())).thenThrow(new ClientConnection.OutboundOverflowException("bob"));
            // when: delivery is attempted in both failure modes
            assertDoesNotThrow(() -> fixture.service.deliverNextMessageIfReady(fixture.connection));
            final var error = assertThrows(ClientConnection.OutboundOverflowException.class,
                    () -> fixture.service.deliverNextMessageIfReady(fixture.connection));
            // then: overflow is identifiable and neither path leaks a lock
            assertTrue(error.getMessage().contains("bob"));
            assertFalse(fixture.lock.isLocked());
        }
    }

    @Test
    void maintenanceAndDisconnectFailuresDoNotEscape() {
        // given: storage and registry operations that fail during background work
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.store.findAckTimedOutMessages(anyLong(), anyInt())).thenThrow(new StorageException("offline", null));
            when(fixture.store.purgeExpired(anyLong())).thenThrow(new StorageException("offline", null));
            when(fixture.store.requeueInFlightMessagesForRecipient("bob")).thenThrow(new StorageException("offline", null));
            when(fixture.registry.registeredConnections()).thenThrow(new IllegalStateException("registry unavailable"));
            // when: timers and disconnect cleanup encounter failures
            assertDoesNotThrow(fixture.service::requeueStaleSafely);
            assertDoesNotThrow(fixture.service::purgeExpiredSafely);
            assertDoesNotThrow(fixture.service::resumePendingDeliveries);
            assertDoesNotThrow(() -> fixture.service.onDisconnect(fixture.connection));
            // then: disconnect still marks and unregisters the connection
            verify(fixture.connection).onClientDisconnected();
            verify(fixture.registry).unregister(fixture.connection);
        }
    }

    @Test
    void retrySkipsLostTransitionsAndOfflineRecipients() {
        // given: one stale row already changed and another whose recipient is offline
        try (final FailureFixture fixture = new FailureFixture()) {
            when(fixture.store.findAckTimedOutMessages(anyLong(), anyInt())).thenReturn(List.of(message(MessageStatus.IN_FLIGHT)));
            when(fixture.store.requeueInFlightMessage(1L)).thenReturn(false, true);
            // when: both scan outcomes are processed
            fixture.service.requeueStaleSafely();
            fixture.service.requeueStaleSafely();
            // then: only the successful transition looks up a recipient and no delivery is attempted
            verify(fixture.registry).findActiveConnection("bob");
            verify(fixture.connection, never()).sendLocked(any());
        }
    }

    @Test
    void executorTasksContainRuntimeFailuresAndRejectedWorkRemainsRetryable() throws Exception {
        // given: a direct submission executor that exposes the wrapper's behavior deterministically
        try (final FailureFixture fixture = new FailureFixture()) {
            final ThreadPoolExecutor executor = fixture.replaceDeliveryExecutor();
            doAnswer(call -> { call.getArgument(0, Runnable.class).run(); return null; }).when(executor).execute(any());
            when(fixture.store.loadNext(eq("bob"), anyLong()))
                    .thenThrow(new ClientConnection.OutboundOverflowException("bob"), new IllegalStateException("unexpected"));
            // when: asynchronous delivery tasks fail or cannot be admitted
            assertDoesNotThrow(() -> fixture.service.startDelivering(fixture.connection));
            assertDoesNotThrow(() -> fixture.service.startDelivering(fixture.connection));
            doThrow(new java.util.concurrent.RejectedExecutionException("full")).when(executor).execute(any());
            assertDoesNotThrow(() -> fixture.service.startDelivering(fixture.connection));
            // then: all three submissions return without leaking errors or locks
            verify(executor, times(3)).execute(any());
            assertFalse(fixture.lock.isLocked());
        }
    }

    @Test
    void shutdownForcesTerminationOnTimeoutOrInterruption() throws Exception {
        // given: an executor that cannot finish normally
        for (final boolean interrupted : new boolean[]{false, true}) {
            try (final FailureFixture fixture = new FailureFixture()) {
                final ThreadPoolExecutor executor = fixture.replaceDeliveryExecutor();
                if (interrupted) when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
                else when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);
                try {
                    // when: closing the delivery service
                    fixture.close();
                    // then: pending work is interrupted and interrupt status is preserved
                    verify(executor).shutdownNow();
                    assertEquals(interrupted, Thread.currentThread().isInterrupted());
                } finally {
                    Thread.interrupted();
                }
            } finally {
                Thread.interrupted();
            }
        }
    }

    private static void assertRejected(final DeliveryService.SendResult result, final SendRejectionReason reason, final boolean retryable) {
        final var rejected = assertInstanceOf(DeliveryService.SendResult.Rejected.class, result);
        assertEquals(reason, rejected.reason());
        assertEquals(retryable, rejected.retryable());
    }

    private static StoredMessage message(final MessageStatus status) {
        return new StoredMessage(1L, MESSAGE_ID, "alice", "bob", "payload", status, 0, 0L, 1);
    }

    private static final class FailureFixture implements AutoCloseable {
        final MailboxStore store = mock(MailboxStore.class);
        final ConnectionRegistry registry = mock(ConnectionRegistry.class);
        final ClientConnection connection = mock(ClientConnection.class);
        final ReentrantLock lock = new ReentrantLock();
        final DeliveryService service = new DeliveryService(RelayConfig.builder().build(), store, registry);
        FailureFixture() {
            when(connection.clientId()).thenReturn("bob");
            when(connection.writeLock()).thenReturn(lock);
        }
        ThreadPoolExecutor replaceDeliveryExecutor() throws Exception {
            final var field = DeliveryService.class.getDeclaredField("deliveryExecutor");
            field.setAccessible(true);
            ((ThreadPoolExecutor) field.get(service)).shutdownNow();
            final ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
            when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
            field.set(service, executor);
            return executor;
        }
        public void close() { service.close(); }
    }
}
