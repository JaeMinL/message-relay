package com.example.relay.store;

/** One row of the MAILBOX table. */
public record StoredMessage(
        long seqNo,
        String messageId,
        String senderId,
        String recipientId,
        String content,
        MessageStatus status,
        long createdAtMillis,
        Long deliveredAtMillis,
        int deliveryAttempts) {

    /**
     * True when this message has already been pushed at least once, so the next
     * push is a redelivery. Read before the attempt counter is incremented.
     */
    public boolean isRedelivery() {
        return deliveryAttempts > 0;
    }
}
