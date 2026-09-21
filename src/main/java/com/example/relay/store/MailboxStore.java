package com.example.relay.store;

import com.example.relay.RelayConfig;
import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Durable mailbox on H2 (file mode), addressed directly over JDBC.
 *
 * <p>Design notes:
 * <ul>
 *   <li>A single MAILBOX table holds every unacknowledged message. There is no
 *       separate per-recipient queue object: the "queue" is the set of rows with
 *       the same RECIPIENT_ID, ordered by SEQ_NO. Reattaching a client after a
 *       reconnect is therefore just re-reading that partition.</li>
 *   <li>SEQ_NO comes from an H2 IDENTITY column, which gives a single global
 *       monotonic order. FIFO per recipient follows from ORDER BY SEQ_NO.</li>
 *   <li>An ACK deletes the row, so there is no retention or tombstone state.</li>
 * </ul>
 */
public final class MailboxStore implements AutoCloseable {

    private static final int POOL_MAX_CONNECTIONS = 16;
    private static final int LOGIN_TIMEOUT_SECONDS = 5;

    private static final String CREATE_MAILBOX_TABLE_QUERY = """
            CREATE TABLE IF NOT EXISTS MAILBOX (
              SEQ_NO             BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
              MESSAGE_ID         VARCHAR(36)   NOT NULL,
              SENDER_ID          VARCHAR(128)  NOT NULL,
              RECIPIENT_ID       VARCHAR(128)  NOT NULL,
              CONTENT            VARCHAR(65536) NOT NULL,
              STATUS             VARCHAR(16)   NOT NULL,
              CREATED_AT         BIGINT        NOT NULL,
              DELIVERED_AT       BIGINT,
              DELIVERY_ATTEMPTS  INT           NOT NULL DEFAULT 0,
              CONSTRAINT UQ_MAILBOX_MESSAGE_ID UNIQUE (MESSAGE_ID)
            )
            """;

    private static final String INSERT_QUEUED_MESSAGE_QUERY = """
            INSERT INTO MAILBOX(
                MESSAGE_ID,
                SENDER_ID,
                RECIPIENT_ID,
                CONTENT,
                STATUS,
                CREATED_AT,
                DELIVERY_ATTEMPTS
            )
            VALUES (?, ?, ?, ?, 'QUEUED', ?, 0)
            """;

    private static final String SELECT_DUPLICATE_MESSAGE_QUERY =
            "SELECT SENDER_ID, RECIPIENT_ID, CONTENT, CREATED_AT FROM MAILBOX WHERE MESSAGE_ID = ?";

    private static final String SELECT_QUEUED_MESSAGES_BY_RECIPIENT_QUERY = """
            SELECT * FROM MAILBOX
             WHERE RECIPIENT_ID = ? AND CREATED_AT >= ?
             ORDER BY SEQ_NO
             LIMIT ?
            """;

    private static final String MARK_QUEUED_MESSAGE_INFLIGHT_QUERY = """
            UPDATE MAILBOX
               SET STATUS = 'IN_FLIGHT', DELIVERED_AT = ?, DELIVERY_ATTEMPTS = DELIVERY_ATTEMPTS + 1
             WHERE SEQ_NO = ? AND STATUS = 'QUEUED'
            """;

    private static final String RESET_INFLIGHT_TO_QUEUED_QUERY = """
            UPDATE MAILBOX SET STATUS = 'QUEUED', DELIVERED_AT = NULL
             WHERE SEQ_NO = ? AND STATUS = 'IN_FLIGHT'
            """;

    private static final String DELETE_ACKNOWLEDGED_MESSAGE_QUERY =
            "DELETE FROM MAILBOX WHERE MESSAGE_ID = ? AND RECIPIENT_ID = ?";

    private static final String SELECT_ACK_TIMED_OUT_MESSAGES_QUERY = """
            SELECT * FROM MAILBOX
             WHERE STATUS = 'IN_FLIGHT' AND DELIVERED_AT < ?
             ORDER BY SEQ_NO
             LIMIT ?
            """;

    private static final String CREATE_RECIPIENT_INDEX_QUERY =
            "CREATE INDEX IF NOT EXISTS IDX_MAILBOX_RECIPIENT ON MAILBOX (RECIPIENT_ID, SEQ_NO)";

    private static final String CREATE_ACK_TIMEOUT_INDEX_QUERY =
            "CREATE INDEX IF NOT EXISTS IDX_MAILBOX_STALE ON MAILBOX (STATUS, DELIVERED_AT)";

    private static final String CREATE_EXPIRY_INDEX_QUERY =
            "CREATE INDEX IF NOT EXISTS IDX_MAILBOX_EXPIRY ON MAILBOX (STATUS, CREATED_AT)";

    private static final String COUNT_UNEXPIRED_RECIPIENT_MESSAGES_QUERY =
            "SELECT COUNT(*) FROM MAILBOX WHERE RECIPIENT_ID = ? AND CREATED_AT >= ?";

    private static final String COUNT_UNEXPIRED_MESSAGES_QUERY =
            "SELECT COUNT(*) FROM MAILBOX WHERE CREATED_AT >= ?";

    private static final String SELECT_MESSAGE_BY_ID_QUERY =
            "SELECT * FROM MAILBOX WHERE MESSAGE_ID = ?";

    private static final String DELETE_EXPIRED_MESSAGES_QUERY =
            "DELETE FROM MAILBOX WHERE CREATED_AT < ?";

    private static final String COUNT_RECIPIENT_MESSAGES_QUERY =
            "SELECT COUNT(*) FROM MAILBOX WHERE RECIPIENT_ID = ?";

    private static final String RESET_RECIPIENT_INFLIGHT_TO_QUEUED_QUERY =
            "UPDATE MAILBOX SET STATUS = 'QUEUED', DELIVERED_AT = NULL WHERE RECIPIENT_ID = ? AND STATUS = 'IN_FLIGHT'";

    private final ReentrantLock admissionLock = new ReentrantLock();

    private final JdbcConnectionPool pool;
    private final RelayConfig config;

    public MailboxStore(final RelayConfig config) {
        this.config = config;
        this.pool = JdbcConnectionPool.create(config.jdbcUrl, "relay", "");
        this.pool.setMaxConnections(POOL_MAX_CONNECTIONS);
        this.pool.setLoginTimeout(LOGIN_TIMEOUT_SECONDS);
        initSchema();
    }

    private void initSchema() {
        try (final Connection connection = pool.getConnection(); final Statement schemaStatement = connection.createStatement()) {
            schemaStatement.execute(CREATE_MAILBOX_TABLE_QUERY);
            // Serves the reconnect replay and the per-mailbox count check.
            schemaStatement.execute(CREATE_RECIPIENT_INDEX_QUERY);
            // Serves findAckTimedOutMessages() every retryScanInterval.
            schemaStatement.execute(CREATE_ACK_TIMEOUT_INDEX_QUERY);
            // Serves purgeExpired() every cleanupInterval.
            schemaStatement.execute(CREATE_EXPIRY_INDEX_QUERY);
        } catch (final SQLException exception) {
            throw new StorageException("failed to initialise schema", exception);
        }
    }

    /**
     * Inserts a message as QUEUED and returns the assigned sequence number.
     *
     * @throws DuplicateIdException if message_id is already taken
     * @throws MailboxFullException if a mailbox or server-wide cap is hit
     * @throws StorageException     on any other storage failure
     */
    public Long enqueue(final String messageId,
                        final String senderId,
                        final String recipientId,
                        final String content,
                        final Long nowMillis
    ) {
        admissionLock.lock();

        try (final Connection dbConnection = pool.getConnection()) {
            dbConnection.setAutoCommit(false);

            try {
                rejectDuplicate(
                        dbConnection,
                        messageId,
                        content,
                        senderId,
                        recipientId,
                        nowMillis
                );

                validateMailboxCapacity(dbConnection, recipientId, nowMillis);

                final long seqNo;
                try (final PreparedStatement preparedStatement =
                             dbConnection.prepareStatement(
                                     INSERT_QUEUED_MESSAGE_QUERY,
                                     Statement.RETURN_GENERATED_KEYS
                             )) {

                    preparedStatement.setString(1, messageId);
                    preparedStatement.setString(2, senderId);
                    preparedStatement.setString(3, recipientId);
                    preparedStatement.setString(4, content);
                    preparedStatement.setLong(5, nowMillis);
                    preparedStatement.executeUpdate();

                    try (final ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                        if (!generatedKeys.next()) {
                            throw new StorageException("insert returned no sequence number", null);
                        }
                        seqNo = generatedKeys.getLong(1);
                    }
                }
                dbConnection.commit();
                return seqNo;
            } catch (final RuntimeException | SQLException exception) {
                safeRollback(dbConnection);
                throw exception;
            }
        } catch (final SQLException exception) {
            throw new StorageException("enqueue failed for " + messageId, exception);
        } finally {
            admissionLock.unlock();
        }
    }

    /**
     * Duplicate message_id handling. The row's age decides whether the sender
     * should retry: an expired row is merely waiting for the cleanup sweep, so
     * the id frees up on its own; a live row means a genuine id collision.
     */
    private void rejectDuplicate(final Connection dbConnection,
                                 final String messageId,
                                 final String content,
                                 final String senderId,
                                 final String recipientId,
                                 final long nowMillis) throws SQLException
    {
        try (final PreparedStatement preparedStatement = dbConnection.prepareStatement(SELECT_DUPLICATE_MESSAGE_QUERY)) {
            preparedStatement.setString(1, messageId);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                if (!queryResult.next()) {
                    return;
                }
                final long createdAtMillis = queryResult.getLong("CREATED_AT");
                final boolean expired = createdAtMillis + config.messageTtl.toMillis() < nowMillis;
                if (expired) {
                    throw new DuplicateIdException(
                            "message_id " + messageId + " belongs to an expired message pending cleanup", true);
                }
                final boolean identical = senderId.equals(queryResult.getString("SENDER_ID"))
                        && recipientId.equals(queryResult.getString("RECIPIENT_ID"))
                        && content.equals(queryResult.getString("CONTENT"));
                // An identical resend is still a conflict, but not retryable: the
                // original is already queued, so resending would duplicate it.
                throw new DuplicateIdException(
                        identical
                                ? "message_id " + messageId + " is already queued with the same payload"
                                : "message_id " + messageId + " is already in use with different data",
                        false);
            }
        }
    }

    private void validateMailboxCapacity(final Connection connection,
                                         final String recipientId,
                                         final long nowMillis) throws SQLException
    {
        final long expiryCutoffMillis = nowMillis - config.messageTtl.toMillis();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(COUNT_UNEXPIRED_RECIPIENT_MESSAGES_QUERY)) {
            preparedStatement.setString(1, recipientId);
            preparedStatement.setLong(2, expiryCutoffMillis);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                queryResult.next();
                if (queryResult.getLong(1) >= config.maxMessagesPerMailbox) {
                    throw new MailboxFullException("mailbox for " + recipientId + " is at its "
                            + config.maxMessagesPerMailbox + " message limit");
                }
            }
        }

        try (final PreparedStatement preparedStatement = connection.prepareStatement(
                COUNT_UNEXPIRED_MESSAGES_QUERY)) {
            preparedStatement.setLong(1, expiryCutoffMillis);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                queryResult.next();
                if (queryResult.getLong(1) >= config.maxPendingMessages) {
                    throw new MailboxFullException("server is at its "
                            + config.maxPendingMessages + " unacknowledged message limit");
                }
            }
        }
    }

    /**
     * Loads the recipient's logical queue in FIFO order, excluding expired messages.
     * Includes both QUEUED and IN_FLIGHT messages until acknowledgement or expiry.
     */
    public List<StoredMessage> loadQueuedMessages(final String recipientId,
                                                  final int limit,
                                                  final long nowMillis)
    {
        try (final Connection dbConnection = pool.getConnection();
             final PreparedStatement preparedStatement = dbConnection.prepareStatement(SELECT_QUEUED_MESSAGES_BY_RECIPIENT_QUERY))
        {
            preparedStatement.setString(1, recipientId);
            preparedStatement.setLong(2, nowMillis - config.messageTtl.toMillis());
            preparedStatement.setInt(3, limit);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                final List<StoredMessage> messages = new ArrayList<>();
                while (queryResult.next()) {
                    messages.add(mapRowToStoredMessage(queryResult));
                }
                return messages;
            }
        } catch (final SQLException exception) {
            throw new StorageException("loadQueuedMessages failed for " + recipientId, exception);
        }
    }

    /** The single oldest unacknowledged message for a recipient, if any. */
    public StoredMessage loadNext(final String recipientId, final long nowMillis) {
        final List<StoredMessage> nextMessages = loadQueuedMessages(recipientId, 1, nowMillis);
        return nextMessages.isEmpty() ? null : nextMessages.get(0);
    }

    /**
     * Marks a message IN_FLIGHT and bumps its attempt counter.
     *
     * @return true if this thread won the transition; false if another thread got
     *         there first, in which case the caller must not write to the stream.
     */
    public boolean markInFlight(final long seqNo, final long nowMillis) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(MARK_QUEUED_MESSAGE_INFLIGHT_QUERY)) {
            preparedStatement.setLong(1, nowMillis);
            preparedStatement.setLong(2, seqNo);
            return preparedStatement.executeUpdate() == 1;
        } catch (final SQLException exception) {
            throw new StorageException("markInFlight failed for seq " + seqNo, exception);
        }
    }

    /** Puts an IN_FLIGHT message back to QUEUED so it can be redelivered. */
    public boolean requeueInFlightMessage(final long seqNo) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(RESET_INFLIGHT_TO_QUEUED_QUERY)) {
            preparedStatement.setLong(1, seqNo);
            return preparedStatement.executeUpdate() == 1;
        } catch (final SQLException exception) {
            throw new StorageException("requeueInFlightMessage failed for seq " + seqNo, exception);
        }
    }

    /** Requeues everything a recipient still owes, used on disconnect and on register. */
    public int requeueInFlightMessagesForRecipient(final String recipientId) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(RESET_RECIPIENT_INFLIGHT_TO_QUEUED_QUERY)) {
            preparedStatement.setString(1, recipientId);

            return preparedStatement.executeUpdate();
        } catch (final SQLException exception) {
            throw new StorageException("requeueInFlightMessagesForRecipient failed for " + recipientId, exception);
        }
    }

    /** Looks a message up by its client-supplied id. */
    public StoredMessage findByMessageId(final String messageId) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_MESSAGE_BY_ID_QUERY)) {
            preparedStatement.setString(1, messageId);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                return queryResult.next() ? mapRowToStoredMessage(queryResult) : null;
            }
        } catch (final SQLException exception) {
            throw new StorageException("findByMessageId failed for " + messageId, exception);
        }
    }

    /**
     * Deletes an acknowledged message. The recipient is part of the predicate so a
     * client can never acknowledge someone else's message.
     *
     * @return true if a row was removed
     */
    public boolean deleteAcknowledged(final String messageId, final String recipientId) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(DELETE_ACKNOWLEDGED_MESSAGE_QUERY)) {
            preparedStatement.setString(1, messageId);
            preparedStatement.setString(2, recipientId);
            return preparedStatement.executeUpdate() == 1;
        } catch (final SQLException exception) {
            throw new StorageException("ack delete failed for " + messageId, exception);
        }
    }

    /** Removes messages older than the TTL. Runs on the cleanup timer. */
    public int purgeExpired(final long nowMillis) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(DELETE_EXPIRED_MESSAGES_QUERY)) {
            preparedStatement.setLong(1, nowMillis - config.messageTtl.toMillis());
            return preparedStatement.executeUpdate();
        } catch (final SQLException exception) {
            throw new StorageException("purgeExpired failed", exception);
        }
    }

    /** IN_FLIGHT messages whose ACK deadline has passed. Runs on the retry timer. */
    public List<StoredMessage> findAckTimedOutMessages(final long nowMillis, final int limit) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ACK_TIMED_OUT_MESSAGES_QUERY)) {
            preparedStatement.setLong(1, nowMillis - config.acknowledgementTimeout.toMillis());
            preparedStatement.setInt(2, limit);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                final List<StoredMessage> messages = new ArrayList<>();
                while (queryResult.next()) {
                    messages.add(mapRowToStoredMessage(queryResult));
                }
                return messages;
            }
        } catch (final SQLException exception) {
            throw new StorageException("findAckTimedOutMessages failed", exception);
        }
    }

    /** Test/diagnostic helper: number of unacknowledged messages for a recipient. */
    public long countUnacknowledgedMessagesForRecipient(final String recipientId) {
        try (final Connection connection = pool.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(
                     COUNT_RECIPIENT_MESSAGES_QUERY)) {
            preparedStatement.setString(1, recipientId);
            try (final ResultSet queryResult = preparedStatement.executeQuery()) {
                queryResult.next();
                return queryResult.getLong(1);
            }
        } catch (final SQLException exception) {
            throw new StorageException("countUnacknowledgedMessagesForRecipient failed for " + recipientId, exception);
        }
    }

    private static StoredMessage mapRowToStoredMessage(final ResultSet queryResult) throws SQLException {
        final long deliveredAtMillis = queryResult.getLong("DELIVERED_AT");
        final Long nullableDeliveredAtMillis = queryResult.wasNull() ? null : deliveredAtMillis;
        return new StoredMessage(
                queryResult.getLong("SEQ_NO"),
                queryResult.getString("MESSAGE_ID"),
                queryResult.getString("SENDER_ID"),
                queryResult.getString("RECIPIENT_ID"),
                queryResult.getString("CONTENT"),
                MessageStatus.valueOf(queryResult.getString("STATUS")),
                queryResult.getLong("CREATED_AT"),
                nullableDeliveredAtMillis,
                queryResult.getInt("DELIVERY_ATTEMPTS"));
    }

    private static void safeRollback(final Connection dbConnection) {
        try {
            dbConnection.rollback();
        } catch (final SQLException ignored) {
            // The connection is being returned to the pool either way.
        }
    }

    @Override
    public void close() {
        pool.dispose();
    }
}
