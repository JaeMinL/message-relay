package com.example.relay.store;

/**
 * Thrown when a Send reuses a message_id that already exists.
 *
 * retryable=true means the existing row is past its TTL and is only waiting for
 * the cleanup sweep, so the same id becomes usable again shortly.
 * retryable=false means a live message already owns that id with different data.
 */
public class DuplicateIdException extends RuntimeException {
    private final boolean retryable;

    public DuplicateIdException(final String message, final boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
