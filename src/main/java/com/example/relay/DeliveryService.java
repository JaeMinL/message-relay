package com.example.relay;

import com.example.relay.proto.Delivery;
import com.example.relay.proto.ProtocolErrorCode;
import com.example.relay.proto.SendRejectionReason;
import com.example.relay.proto.ServerEvent;
import com.example.relay.store.DuplicateIdException;
import com.example.relay.store.MailboxFullException;
import com.example.relay.store.MailboxStore;
import com.example.relay.store.MessageStatus;
import com.example.relay.store.StorageException;
import com.example.relay.store.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles registration, delivery, acknowledgements and retries in recipient FIFO order.
 * Connection write locks protect delivery state, with one pending message per recipient.
 */
public final class DeliveryService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);
    private static final int STALE_SCAN_BATCH = 500;

    private final RelayConfig config;
    private final MailboxStore mailboxStore;
    private final ConnectionRegistry registry;
    private final ThreadPoolExecutor deliveryExecutor;
    private final ScheduledExecutorService maintenance;

    /**
     * Creates delivery executors without starting maintenance timers.
     * The caller remains responsible for closing the mailbox store.
     */
    public DeliveryService(final RelayConfig config, final MailboxStore mailboxStore, final ConnectionRegistry registry) {
        this.config = config;
        this.mailboxStore = mailboxStore;
        this.registry = registry;
        this.deliveryExecutor = new ThreadPoolExecutor(
                config.executorThreads,
                config.executorThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.executorQueueCapacity),
                Thread.ofPlatform().name("relay-delivery-", 0).factory(),
                // Surface saturation to submitDeliveryTask(), while the durable message remains
                // QUEUED until resumePendingDeliveries() schedules it again.
                new ThreadPoolExecutor.AbortPolicy());
        this.maintenance = Executors.newScheduledThreadPool(
                2, Thread.ofPlatform().name("relay-maintenance-", 0).factory());
    }

    /**
     * Starts periodic retries, expiry cleanup and pending delivery recovery.
     * Call once before closing the service; starting after closure throws an exception.
     */
    public void start() {
        log.info("Starting delivery retry, cleanup and recovery timers");
        maintenance.scheduleWithFixedDelay(this::requeueStaleSafely,
                config.retryScanInterval.toMillis(),
                config.retryScanInterval.toMillis(), TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(this::purgeExpiredSafely,
                config.cleanupInterval.toMillis(),
                config.cleanupInterval.toMillis(), TimeUnit.MILLISECONDS);
        maintenance.scheduleWithFixedDelay(this::resumePendingDeliveries,
                config.retryScanInterval.toMillis(),
                config.retryScanInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------ register

    /**
     * Requeues messages from a previous connection and returns the pending message count.
     * Delivery starts separately after registration confirmation. Storage failures propagate.
     */
    public long restoreMailboxForConnection(final ClientConnection connection) {
        final String clientId = connection.clientId();
        mailboxStore.requeueInFlightMessagesForRecipient(clientId);
        log.info("Recovered mailbox for client={}", clientId);
        return mailboxStore.countUnacknowledgedMessagesForRecipient(clientId);
    }

    /**
     * Schedules mailbox delivery after registration confirmation has been sent.
     */
    public void startDelivering(final ClientConnection connection) {
        submitDeliveryTask(() -> deliverNextMessageIfReady(connection));
    }

    // ---------------------------------------------------------------------- send

    /** Result of validating and attempting to persist a message. */
    public sealed interface SendResult {
        /**
         * Contains the sequence number of a stored message; delivery may still be pending.
         */
        record Accepted(long seqNo) implements SendResult {}
        /**
         * Describes a rejected send request and whether the sender may retry it.
         */
        record Rejected(SendRejectionReason reason, boolean retryable, String detail) implements SendResult {}
    }

    /**
     * Validates and stores a message, then schedules delivery to a connected recipient.
     * Returns acceptance after persistence or a rejection describing the failure.
     */
    public SendResult onSend(final String senderId, final String messageId, final String recipientId, final String content) {
        if (!isUuid(messageId)) {
            return reject(SendRejectionReason.INVALID_MESSAGE, false, "message_id must be a UUID");
        }
        if (!isValidClientId(recipientId)) {
            return reject(SendRejectionReason.INVALID_MESSAGE, false, "invalid recipient_id");
        }
        if (content == null || content.isEmpty()) {
            return reject(SendRejectionReason.INVALID_MESSAGE, false, "content must not be empty");
        }
        final int contentBytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (contentBytes > config.maxMessageContentSize) {
            return reject(SendRejectionReason.MESSAGE_TOO_LARGE, false,
                    contentBytes + " bytes exceeds the " + config.maxMessageContentSize + " byte limit");
        }

        final long seqNo;
        try {
            seqNo = mailboxStore.enqueue(messageId, senderId, recipientId, content, System.currentTimeMillis());
            log.info("Queued message with seqNo={} for recipient={}", seqNo, recipientId);
        } catch (final DuplicateIdException e) {
            return reject(SendRejectionReason.DUPLICATE_ID_CONFLICT, e.isRetryable(), e.getMessage());
        } catch (final MailboxFullException e) {
            return reject(SendRejectionReason.RESOURCE_EXHAUSTED, true, e.getMessage());
        } catch (final StorageException e) {
            log.warn("storage failure while enqueueing {}", messageId, e);
            return reject(SendRejectionReason.STORAGE_FAILURE, true, "could not persist the message");
        }

        // Push off the caller's thread so one recipient's stream can never slow
        // down the sender's request handling.
        final ClientConnection recipient = registry.findActiveConnection(recipientId);
        if (recipient != null) {
            submitDeliveryTask(() -> deliverNextMessageIfReady(recipient));
        }
        return new SendResult.Accepted(seqNo);
    }

    // ----------------------------------------------------------------------- ack

    /** Result of processing an acknowledgement, including protocol and storage failures. */
    public enum AckOutcome {
        /** Row deleted, next message pumped. */
        ACKED,
        /** Malformed id, or no such message (already acknowledged, expired, purged). */
        UNKNOWN,
        /** The message exists but belongs to a different recipient. */
        WRONG_RECIPIENT,
        /** The message exists for this recipient but is not awaiting an ACK. */
        NOT_IN_FLIGHT,
        /** Storage could not be reached. */
        STORAGE_ERROR
    }

    /**
     * Handles an acknowledgement and schedules the next delivery after deletion.
     * Missing messages return UNKNOWN; queued messages return NOT_IN_FLIGHT.
     * Storage failures return STORAGE_ERROR rather than propagating an exception.
     */
    public AckOutcome onAck(final ClientConnection connection, final String messageId) {
        if (!isUuid(messageId)) {
            return AckOutcome.UNKNOWN;
        }
        try {
            final StoredMessage message = mailboxStore.findByMessageId(messageId);
            if (message == null) {
                return AckOutcome.UNKNOWN;
            }
            if (!message.recipientId().equals(connection.clientId())) {
                return AckOutcome.WRONG_RECIPIENT;
            }
            if (message.status() != MessageStatus.IN_FLIGHT) {
                return AckOutcome.NOT_IN_FLIGHT;
            }
            if (!mailboxStore.deleteAcknowledged(messageId, connection.clientId())) {
                // Lost the race with the cleanup sweep; the effect is the same.
                return AckOutcome.UNKNOWN;
            }

            connection.writeLock().lock();
            try {
                final Long inFlight = connection.inFlightSeqNo();
                if (inFlight != null && inFlight == message.seqNo()) {
                    connection.setInFlightSeqNo(null);
                }
            } finally {
                connection.writeLock().unlock();
            }
            submitDeliveryTask(() -> deliverNextMessageIfReady(connection));
            log.info("Acknowledged and deleted message with seqNo={}", message.seqNo());
            return AckOutcome.ACKED;
        } catch (final StorageException e) {
            log.warn("storage failure while acknowledging {}", messageId, e);
            return AckOutcome.STORAGE_ERROR;
        }
    }

    // ---------------------------------------------------------------- disconnect

    /**
     * Unregisters a disconnected client and requeues its pending delivery for reconnect.
     * Storage failures are logged and left for the retry scan to recover.
     */
    public void onDisconnect(final ClientConnection connection) {
        connection.onClientDisconnected();
        registry.unregister(connection);
        log.info("Disconnected and unregistered client={}", connection.clientId());
        try {
            mailboxStore.requeueInFlightMessagesForRecipient(connection.clientId());
        } catch (final StorageException e) {
            // The retry scan will requeue it later; nothing is lost.
            log.warn("could not requeue in-flight messages for {}", connection.clientId(), e);
        }
    }

    // --------------------------------------------------------------------- pumps

    /**
     * Sends the oldest unexpired message when no valid delivery is awaiting ACK.
     * Clears expired or deleted delivery slots under the connection write lock.
     * Skips busy connections, logs storage failures and propagates outbound overflow.
     */
    void deliverNextMessageIfReady(final ClientConnection connection) {
        final ReentrantLock lock = connection.writeLock();
        if (!lock.tryLock()) {
            // Periodic recovery retries if the current lock holder does not pump.
            return;
        }
        try {
            if (connection.isClosed()) {
                return;
            }
            final StoredMessage messageToDeliver = mailboxStore.loadNext(connection.clientId(), System.currentTimeMillis());
            final Long inFlightSeqNo = connection.inFlightSeqNo();

            if (inFlightSeqNo != null) {
                // Waiting for ACK
                if (messageToDeliver != null && inFlightSeqNo == messageToDeliver.seqNo()) {
                    return;
                }
                // The pending message has expired or been deleted. Keep this reconciliation under the same lock as delivery and ACK updates.
                connection.setInFlightSeqNo(null);
                log.info("Released expired or deleted delivery slot with seqNo={} for client={}",
                        inFlightSeqNo, connection.clientId());
            }
            if (messageToDeliver == null) {
                return;
            }
            if (!mailboxStore.markInFlight(messageToDeliver.seqNo(), System.currentTimeMillis())) {
                // Someone else transitioned it; do not write a second copy now.
                return;
            }
            final ServerEvent event = ServerEvent.newBuilder().setDelivery(Delivery.newBuilder()
                            .setMessageId(messageToDeliver.messageId())
                            .setSenderId(messageToDeliver.senderId())
                            .setContent(messageToDeliver.content())
                            .setSeqNo(messageToDeliver.seqNo())
                            .setRedelivery(messageToDeliver.isRedelivery())
                            .setSentAtEpochMillis(messageToDeliver.createdAtMillis())
                            .build()).build();

            if (connection.sendLocked(event)) {
                connection.setInFlightSeqNo(messageToDeliver.seqNo());
                log.info("Delivered message with seqNo={}, redelivery={} to client={}",
                        messageToDeliver.seqNo(), messageToDeliver.isRedelivery(), connection.clientId());
            } else {
                // Write failed: undo the transition so the message is not stuck
                // IN_FLIGHT for the full ACK timeout.
                if (mailboxStore.requeueInFlightMessage(messageToDeliver.seqNo())) {
                    log.info("Requeued message with seqNo={} after failed delivery", messageToDeliver.seqNo());
                }
            }
        } catch (final ClientConnection.OutboundOverflowException e) {
            throw e;
        } catch (final StorageException e) {
            log.warn("storage failure while delivering to {}", connection.clientId(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Requeues a batch of messages past their ACK deadline and schedules redelivery.
     * Updates pending slots under connection locks and logs failures to keep the timer running.
     */
    void requeueStaleSafely() {
        try {
            final List<StoredMessage> stale = mailboxStore.findAckTimedOutMessages(System.currentTimeMillis(), STALE_SCAN_BATCH);
            for (final StoredMessage message : stale) {
                if (!mailboxStore.requeueInFlightMessage(message.seqNo())) {
                    continue;
                }
                log.info("Requeued message with seqNo={} after ACK timeout", message.seqNo());
                final ClientConnection connection = registry.findActiveConnection(message.recipientId());
                if (connection == null) {
                    continue;
                }
                connection.writeLock().lock();
                try {
                    final Long inFlight = connection.inFlightSeqNo();
                    if (inFlight != null && inFlight == message.seqNo()) {
                        connection.setInFlightSeqNo(null);
                    }
                } finally {
                    connection.writeLock().unlock();
                }
                submitDeliveryTask(() -> deliverNextMessageIfReady(connection));
            }
        } catch (final RuntimeException e) {
            // A scheduled task that throws is cancelled, so swallow and retry later.
            log.warn("retry scan failed", e);
        }
    }

    /**
     * Deletes expired messages and schedules delivery for connected clients.
     * Logs failures so later cleanup sweeps can still run.
     */
    void purgeExpiredSafely() {
        try {
            final int removed = mailboxStore.purgeExpired(System.currentTimeMillis());
            if (removed > 0) {
                log.info("purged {} expired message(s)", removed);
                resumePendingDeliveries();
            }
        } catch (final RuntimeException e) {
            log.warn("cleanup sweep failed", e);
        }
    }

    /**
     * Retries delivery for connected clients after executor rejection or message expiry.
     * Skips closed connections and logs failures to keep the timer running.
     */
    void resumePendingDeliveries() {
        try {
            for (final ClientConnection connection : registry.registeredConnections()) {
                if (!connection.isClosed()) {
                    submitDeliveryTask(() -> deliverNextMessageIfReady(connection));
                }
            }
        } catch (final RuntimeException e) {
            // A scheduled task that throws is cancelled, so keep later runs alive.
            log.warn("could not resume pending deliveries", e);
        }
    }

    /**
     * Submits delivery work and logs task failures or executor rejection.
     * Rejected work relies on periodic mailbox recovery for another attempt.
     */
    private void submitDeliveryTask(final Runnable task) {
        try {
            deliveryExecutor.execute(() -> {
                try {
                    task.run();
                } catch (final ClientConnection.OutboundOverflowException e) {
                    log.warn("{}", e.getMessage());
                } catch (final RuntimeException e) {
                    log.warn("delivery task failed", e);
                }
            });
        } catch (final RejectedExecutionException e) {
            log.warn("delivery task rejected; pending delivery will be rescheduled");
        }
    }

    // ---------------------------------------------------------------- validation

    /**
     * Checks whether a client ID is nonblank and fits the configured UTF-8 byte limit.
     * Null IDs are invalid.
     */
    public boolean isValidClientId(final String clientId) {
        if (clientId == null || clientId.isBlank()) {
            log.warn("Client ID validation failed because the ID is missing or blank");
            return false;
        }
        final int clientIdBytes = clientId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (clientIdBytes > config.maxClientIdSize) {
            log.warn("Client ID validation failed with bytes={} exceeding limit={}", clientIdBytes, config.maxClientIdSize);
            return false;
        }
        return true;
    }

    /**
     * Checks whether a value is 36 characters long and can be parsed as a UUID.
     * Returns false for null or malformed input.
     */
    private static boolean isUuid(final String value) {
        if (value == null || value.length() != 36) {
            return false;
        }
        try {
            UUID.fromString(value);
            return true;
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Creates a send rejection with its reason, retryability and explanation.
     */
    private static SendResult reject(final SendRejectionReason reason, final boolean retryable, final String detail) {
        log.warn("Send validation or storage rejected request with reason={}, retryable={}", reason, retryable);
        return new SendResult.Rejected(reason, retryable, detail);
    }

    /**
     * Maps an acknowledgement result to its protocol error code.
     * Successful acknowledgements map to the unspecified error code.
     */
    public static ProtocolErrorCode toProtocolErrorCode(final AckOutcome outcome) {
        return switch (outcome) {
            case UNKNOWN -> ProtocolErrorCode.ACK_UNKNOWN;
            case WRONG_RECIPIENT -> ProtocolErrorCode.ACK_WRONG_RECIPIENT;
            case NOT_IN_FLIGHT -> ProtocolErrorCode.ACK_NOT_IN_FLIGHT;
            case STORAGE_ERROR -> ProtocolErrorCode.STORAGE_UNAVAILABLE;
            case ACKED -> ProtocolErrorCode.PROTOCOL_ERROR_CODE_UNSPECIFIED;
        };
    }

    /**
     * Stops maintenance and waits up to the configured grace period for delivery work.
     * Forces executor shutdown on timeout or interruption, preserving interrupt status.
     * The mailbox store and client connections remain owned by the caller.
     */
    @Override
    public void close() {
        log.info("Stopping delivery service executors");
        maintenance.shutdownNow();
        deliveryExecutor.shutdown();
        try {
            if (!deliveryExecutor.awaitTermination(config.shutdownGracePeriod.toSeconds(), TimeUnit.SECONDS)) {
                log.warn("delivery executor did not drain within the grace period");
                deliveryExecutor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            deliveryExecutor.shutdownNow();
        }
    }
}
