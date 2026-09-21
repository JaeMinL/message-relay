package com.example.relay.store;

/** Per-recipient or server-wide unacknowledged message cap exceeded. */
public class MailboxFullException extends RuntimeException {
    public MailboxFullException(final String message) {
        super(message);
    }
}
