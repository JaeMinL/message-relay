package com.example.relay.store;

/** Wraps any SQLException so callers never see JDBC types. */
public class StorageException extends RuntimeException {
    public StorageException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
