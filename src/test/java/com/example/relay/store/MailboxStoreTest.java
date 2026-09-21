package com.example.relay.store;

import com.example.relay.RelayConfig;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MailboxStoreTest {

    @Test
    void expiredDuplicateCanBeRetriedOnlyAfterCleanup() {
        // given: an expired row that still reserves its unique message ID
        final RelayConfig config = RelayConfig.builder().jdbcUrl("jdbc:h2:mem:" + UUID.randomUUID())
                .messageTtl(Duration.ofMillis(100)).maxMessagesPerMailbox(1).build();
        try (final MailboxStore store = new MailboxStore(config)) {
            final String messageId = UUID.randomUUID().toString();
            store.enqueue(messageId, "alice", "bob", "old", 0L);
            // when: the expired ID is reused before cleanup
            final var duplicate = assertThrows(DuplicateIdException.class,
                    () -> store.enqueue(messageId, "alice", "bob", "new", 101L));
            // then: it is retryable, excluded from delivery and capacity, and reusable after deletion
            assertTrue(duplicate.isRetryable());
            assertNull(store.loadNext("bob", 101L));
            assertEquals(1, store.countUnacknowledgedMessagesForRecipient("bob"));
            final String anotherId = UUID.randomUUID().toString();
            store.enqueue(anotherId, "alice", "bob", "active", 101L);
            assertEquals(1, store.loadQueuedMessages("bob", 10, 101L).size());
            assertEquals(1, store.purgeExpired(101L));
            assertTrue(store.deleteAcknowledged(anotherId, "bob"));
            assertNotNull(store.enqueue(messageId, "alice", "bob", "new", 101L));
        }
    }

    @Test
    void identicalLiveDuplicateIsNotRetryableAndDoesNotOverwriteContent() {
        // given: a persisted message
        final RelayConfig config = RelayConfig.builder().jdbcUrl("jdbc:h2:mem:" + UUID.randomUUID()).build();
        try (final MailboxStore store = new MailboxStore(config)) {
            final String messageId = UUID.randomUUID().toString();
            store.enqueue(messageId, "alice", "bob", "content", 0L);
            // when: the same payload is submitted again
            final var duplicate = assertThrows(DuplicateIdException.class,
                    () -> store.enqueue(messageId, "alice", "bob", "content", 0L));
            // then: the original remains the only row and blind retries are discouraged
            assertFalse(duplicate.isRetryable());
            assertTrue(duplicate.getMessage().contains("same payload"));
            assertEquals(1, store.countUnacknowledgedMessagesForRecipient("bob"));
            assertEquals("content", store.findByMessageId(messageId).content());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"enqueue", "queued", "mark", "requeue", "recipient", "find", "delete", "purge", "stale", "count"})
    void sqlFailuresAreWrappedWithTheirCause(final String operation) throws Exception {
        // given: a successfully initialized pool whose next connection acquisition fails
        final JdbcConnectionPool pool = mock(JdbcConnectionPool.class);
        final Connection connection = mock(Connection.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(mock(Statement.class));
        final SQLException failure = new SQLException("database unavailable");
        try (final var factory = mockStatic(JdbcConnectionPool.class)) {
            factory.when(() -> JdbcConnectionPool.create(anyString(), anyString(), anyString())).thenReturn(pool);
            try (final MailboxStore store = new MailboxStore(RelayConfig.builder().build())) {
                when(pool.getConnection()).thenThrow(failure);
                // when: each public storage operation encounters the same JDBC failure
                final StorageException error = assertThrows(StorageException.class, () -> {
                    switch (operation) {
                        case "enqueue" -> store.enqueue("id", "alice", "bob", "x", 0L);
                        case "queued" -> store.loadQueuedMessages("bob", 1, 0);
                        case "mark" -> store.markInFlight(1, 0);
                        case "requeue" -> store.requeueInFlightMessage(1);
                        case "recipient" -> store.requeueInFlightMessagesForRecipient("bob");
                        case "find" -> store.findByMessageId("id");
                        case "delete" -> store.deleteAcknowledged("id", "bob");
                        case "purge" -> store.purgeExpired(0);
                        case "stale" -> store.findAckTimedOutMessages(0, 1);
                        case "count" -> store.countUnacknowledgedMessagesForRecipient("bob");
                        default -> fail("unknown operation");
                    }
                });
                // then: callers receive a storage-level exception with the JDBC cause
                assertSame(failure, error.getCause());
            }
            verify(pool).dispose();
        }
    }

    @Test
    void schemaFailureIsReportedAsStorageFailure() throws Exception {
        // given: a pool whose schema connection cannot be opened
        final JdbcConnectionPool pool = mock(JdbcConnectionPool.class);
        final SQLException failure = new SQLException("schema unavailable");
        when(pool.getConnection()).thenThrow(failure);
        try (final var factory = mockStatic(JdbcConnectionPool.class)) {
            factory.when(() -> JdbcConnectionPool.create(anyString(), anyString(), anyString())).thenReturn(pool);
            // when: storage initialization fails
            final var error = assertThrows(StorageException.class, () -> new MailboxStore(RelayConfig.builder().build()));
            // then: the initialization failure retains its SQL cause
            assertSame(failure, error.getCause());
            assertTrue(error.getMessage().contains("schema"));
        }
    }

    @Test
    void missingGeneratedKeyRollsBackEvenIfRollbackAlsoFails() throws Exception {
        // given: an insert that returns no generated sequence number
        final JdbcConnectionPool pool = mock(JdbcConnectionPool.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(mock(Statement.class));
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.prepareStatement(anyString(), anyInt())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(mock(ResultSet.class));
        when(statement.getGeneratedKeys()).thenReturn(mock(ResultSet.class));
        doThrow(new SQLException("rollback also failed")).when(connection).rollback();
        try (final var factory = mockStatic(JdbcConnectionPool.class)) {
            factory.when(() -> JdbcConnectionPool.create(anyString(), anyString(), anyString())).thenReturn(pool);
            try (final MailboxStore store = new MailboxStore(RelayConfig.builder().build())) {
                // when: enqueue cannot obtain the inserted row's identity
                final var error = assertThrows(StorageException.class,
                        () -> store.enqueue("id", "alice", "bob", "x", 0L));
                // then: the original failure survives and commit is never attempted
                assertTrue(error.getMessage().contains("sequence number"));
                verify(connection).rollback();
                verify(connection, never()).commit();
            }
        }
    }

    @Test
    void globalCapacityRejectsInsertBeforeWriting() throws Exception {
        // given: the recipient has room but the global mailbox count reaches its limit
        final JdbcConnectionPool pool = mock(JdbcConnectionPool.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ResultSet queryResult = mock(ResultSet.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(mock(Statement.class));
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(queryResult);
        when(queryResult.next()).thenReturn(false, true, true);
        when(queryResult.getLong(1)).thenReturn(0L, 100_000L);
        try (final var factory = mockStatic(JdbcConnectionPool.class)) {
            factory.when(() -> JdbcConnectionPool.create(anyString(), anyString(), anyString())).thenReturn(pool);
            try (final MailboxStore store = new MailboxStore(RelayConfig.builder().build())) {
                // when: a new message would exceed the global limit
                final var error = assertThrows(MailboxFullException.class,
                        () -> store.enqueue("id", "alice", "bob", "x", 0L));
                // then: the transaction rolls back without executing an insert
                assertTrue(error.getMessage().contains("server"));
                verify(statement, never()).executeUpdate();
                verify(connection).rollback();
            }
        }
    }

@Test
    void duplicateUuidWithDifferentSenderPreservesOriginalMessage() {
        // given: a stored message with a globally unique UUID
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the UUID is reused with a different sender
            final DuplicateIdException error = assertThrows(DuplicateIdException.class,
                    () -> fixture.store.enqueue(fixture.messageId, "carol", "bob", "payload", 10L));
            // then: the conflict is nonretryable and neither the row nor mailbox ownership changes
            assertFalse(error.isRetryable());
            assertTrue(error.getMessage().contains("different data"));
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
            assertEquals(1, fixture.store.countUnacknowledgedMessagesForRecipient("bob"));
            assertEquals(0, fixture.store.countUnacknowledgedMessagesForRecipient("carol"));
        }
    }

    @Test
    void duplicateUuidWithDifferentRecipientPreservesOriginalMessage() {
        // given: a stored message with a globally unique UUID
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the UUID is reused with a different recipient
            final DuplicateIdException error = assertThrows(DuplicateIdException.class,
                    () -> fixture.store.enqueue(fixture.messageId, "alice", "carol", "payload", 10L));
            // then: the conflict is nonretryable and neither the row nor mailbox ownership changes
            assertFalse(error.isRetryable());
            assertTrue(error.getMessage().contains("different data"));
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
            assertEquals(1, fixture.store.countUnacknowledgedMessagesForRecipient("bob"));
            assertEquals(0, fixture.store.countUnacknowledgedMessagesForRecipient("carol"));
        }
    }

    @Test
    void duplicateUuidWithDifferentContentPreservesOriginalMessage() {
        // given: a stored message with a globally unique UUID
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the UUID is reused with a different content
            final DuplicateIdException error = assertThrows(DuplicateIdException.class,
                    () -> fixture.store.enqueue(fixture.messageId, "alice", "bob", "changed", 10L));
            // then: the conflict is nonretryable and neither the row nor mailbox ownership changes
            assertFalse(error.isRetryable());
            assertTrue(error.getMessage().contains("different data"));
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
            assertEquals(1, fixture.store.countUnacknowledgedMessagesForRecipient("bob"));
            assertEquals(0, fixture.store.countUnacknowledgedMessagesForRecipient("carol"));
        }
    }

    @Test
    void markMissingMessageReturnsFalseWithoutChangingOtherRows() {
        // given: one queued row
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: a nonexistent sequence is claimed
            final boolean claimed = fixture.store.markInFlight(original.seqNo() + 100, 20L);
            // then: no row is changed
            assertFalse(claimed);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
        }
    }

    @Test
    void markAlreadyInflightMessageDoesNotIncrementAttemptsOrResetDeadline() {
        // given: a message already awaiting ACK
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage queued = fixture.enqueue();
            assertTrue(fixture.store.markInFlight(queued.seqNo(), 20L));
            final StoredMessage original = fixture.store.findByMessageId(fixture.messageId);
            // when: another worker attempts to claim the same row
            final boolean claimed = fixture.store.markInFlight(queued.seqNo(), 99L);
            // then: the original attempt count and timestamp are preserved
            assertFalse(claimed);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
        }
    }

    @Test
    void requeueMissingMessageReturnsFalseWithoutChangingOtherRows() {
        // given: one queued row
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: a nonexistent sequence is requeued
            final boolean requeued = fixture.store.requeueInFlightMessage(original.seqNo() + 100);
            // then: no row is changed
            assertFalse(requeued);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
        }
    }

    @Test
    void requeueAlreadyQueuedMessageDoesNotChangeItsState() {
        // given: one queued row
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the queued row is requeued again
            final boolean requeued = fixture.store.requeueInFlightMessage(original.seqNo());
            // then: the no-op is reported and the complete row is unchanged
            assertFalse(requeued);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
        }
    }

    @Test
    void ackFromWrongRecipientCannotDeleteMessage() {
        // given: a message awaiting bob's acknowledgement
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage queued = fixture.enqueue();
            assertTrue(fixture.store.markInFlight(queued.seqNo(), 20L));
            final StoredMessage original = fixture.store.findByMessageId(fixture.messageId);
            // when: carol attempts to delete bob's message
            final boolean deleted = fixture.store.deleteAcknowledged(fixture.messageId, "carol");
            // then: recipient filtering preserves the entire original row
            assertFalse(deleted);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
            assertEquals(1, fixture.store.countUnacknowledgedMessagesForRecipient("bob"));
        }
    }

    @Test
    void ackForUnknownUuidDoesNotDeleteExistingMessage() {
        // given: an existing message
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: an unknown UUID is acknowledged
            final boolean deleted = fixture.store.deleteAcknowledged(UUID.randomUUID().toString(), "bob");
            // then: the unrelated row remains unchanged
            assertFalse(deleted);
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
        }
    }

    @Test
    void repeatedAckReturnsFalseAfterSuccessfulDeletion() {
        // given: an acknowledged row already removed from storage
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage queued = fixture.enqueue();
            assertTrue(fixture.store.markInFlight(queued.seqNo(), 20L));
            assertTrue(fixture.store.deleteAcknowledged(fixture.messageId, "bob"));
            // when: the same ACK arrives again
            final boolean deleted = fixture.store.deleteAcknowledged(fixture.messageId, "bob");
            // then: no second deletion occurs and the mailbox stays empty
            assertFalse(deleted);
            assertNull(fixture.store.findByMessageId(fixture.messageId));
            assertEquals(0, fixture.store.countUnacknowledgedMessagesForRecipient("bob"));
        }
    }

    @Test
    void uuidAtExactTtlBoundaryIsStillALiveConflict() {
        // given: a row created at zero with a 100ms TTL
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the UUID is reused exactly at its TTL boundary
            final DuplicateIdException error = assertThrows(DuplicateIdException.class,
                    () -> fixture.store.enqueue(fixture.messageId, "alice", "bob", "payload", 100L));
            // then: the inclusive boundary remains live and cleanup must not delete it
            assertFalse(error.isRetryable());
            assertEquals(original, fixture.store.loadNext("bob", 100L));
            assertEquals(0, fixture.store.purgeExpired(100L));
        }
    }

    @Test
    void expiredUuidRejectionDoesNotDeleteOrOverwriteTheOldRow() {
        // given: an unswept row older than its TTL
        try (final StateFixture fixture = new StateFixture()) {
            final StoredMessage original = fixture.enqueue();
            // when: the expired UUID is reused
            final DuplicateIdException error = assertThrows(DuplicateIdException.class,
                    () -> fixture.store.enqueue(fixture.messageId, "carol", "dave", "new", 101L));
            // then: retry is allowed after cleanup but this failed request has no side effects
            assertTrue(error.isRetryable());
            assertEquals(original, fixture.store.findByMessageId(fixture.messageId));
            assertNull(fixture.store.loadNext("bob", 101L));
            assertEquals(0, fixture.store.countUnacknowledgedMessagesForRecipient("dave"));
        }
    }

    private static final class StateFixture implements AutoCloseable {
        final String messageId = UUID.randomUUID().toString();
        final MailboxStore store = new MailboxStore(RelayConfig.builder()
                .jdbcUrl("jdbc:h2:mem:state-" + UUID.randomUUID())
                .messageTtl(Duration.ofMillis(100)).build());
        StoredMessage enqueue() {
            store.enqueue(messageId, "alice", "bob", "payload", 0L);
            return store.findByMessageId(messageId);
        }
        public void close() { store.close(); }
    }

@Test
    void concurrentClaimsOnlyIncrementDeliveryAttemptsOnce() throws Exception {
        // given: one queued message and two workers trying to deliver it
        try (final MailboxStore store = newConcurrentStore()) {
            final String messageId = UUID.randomUUID().toString();
            final long seqNo = store.enqueue(messageId, "alice", "bob", "payload", 0L);
            // when: both workers claim the same message concurrently
            final List<Boolean> results = race(
                    () -> store.markInFlight(seqNo, 10L),
                    () -> store.markInFlight(seqNo, 20L));
            // then: exactly one claim succeeds and its timestamp and attempt count are retained
            assertNotEquals(results.get(0), results.get(1));
            final StoredMessage stored = store.findByMessageId(messageId);
            assertEquals(MessageStatus.IN_FLIGHT, stored.status());
            assertEquals(1, stored.deliveryAttempts());
            assertEquals(Long.valueOf(results.get(0) ? 10L : 20L), stored.deliveredAtMillis());
            assertEquals(1, store.countUnacknowledgedMessagesForRecipient("bob"));
        }
    }

    @Test
    void concurrentDuplicateUuidSendsPersistOnlyTheWinningRequest() throws Exception {
        // given: two independent senders reuse the same UUID
        try (final MailboxStore store = newConcurrentStore()) {
            final String messageId = UUID.randomUUID().toString();
            // when: both attempt insertion at the same time
            final List<Boolean> results = race(
                    () -> enqueueOrReject(store, messageId, "alice"),
                    () -> enqueueOrReject(store, messageId, "carol"));
            // then: one request wins and the rejected request cannot overwrite it
            assertNotEquals(results.get(0), results.get(1));
            final StoredMessage stored = store.findByMessageId(messageId);
            assertEquals(results.get(0) ? "alice" : "carol", stored.senderId());
            assertEquals("payload", stored.content());
            assertEquals(MessageStatus.QUEUED, stored.status());
            assertEquals(0, stored.deliveryAttempts());
            assertNull(stored.deliveredAtMillis());
            assertEquals(1, store.countUnacknowledgedMessagesForRecipient("bob"));
        }
    }

    private static boolean enqueueOrReject(final MailboxStore store, final String messageId, final String sender) {
        try {
            store.enqueue(messageId, sender, "bob", "payload", 0L);
            return true;
        } catch (final DuplicateIdException rejected) {
            assertFalse(rejected.isRetryable());
            return false;
        }
    }

    private static List<Boolean> race(final Callable<Boolean> first, final Callable<Boolean> second) throws Exception {
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);
        try (final var workers = Executors.newFixedThreadPool(2)) {
            final var firstResult = workers.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return first.call();
            });
            final var secondResult = workers.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, TimeUnit.SECONDS));
                return second.call();
            });
            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }
            return List.of(firstResult.get(5, TimeUnit.SECONDS), secondResult.get(5, TimeUnit.SECONDS));
        }
    }

    private static MailboxStore newConcurrentStore() {
        return new MailboxStore(RelayConfig.builder().jdbcUrl("jdbc:h2:mem:concurrency-" + UUID.randomUUID()).build());
    }
}
