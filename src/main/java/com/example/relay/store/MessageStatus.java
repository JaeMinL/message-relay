package com.example.relay.store;

/**
 * QUEUED    - accepted and durable, not yet pushed to the recipient.
 * IN_FLIGHT - pushed to the recipient, waiting for an ACK.
 *
 * There is no ACKED state: a successful ACK deletes the row.
 */
public enum MessageStatus {
    QUEUED,
    IN_FLIGHT
}
