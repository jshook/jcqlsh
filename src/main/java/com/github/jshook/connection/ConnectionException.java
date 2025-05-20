package com.github.jshook.connection;

/**
 * Exception thrown when a connection to Cassandra fails.
 */
public class ConnectionException extends Exception {
    /**
     * Creates a new ConnectionException with the given message.
     * @param message the error message
     */
    public ConnectionException(String message) {
        super(message);
    }

    /**
     * Creates a new ConnectionException with the given message and cause.
     * @param message the error message
     * @param cause the cause of the exception
     */
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
