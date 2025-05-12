package org.cqlsh.connection;

/**
 * Exception thrown when a query execution fails.
 */
public class QueryExecutionException extends Exception {
    /**
     * Creates a new QueryExecutionException with the given message.
     * @param message the error message
     */
    public QueryExecutionException(String message) {
        super(message);
    }
    
    /**
     * Creates a new QueryExecutionException with the given message and cause.
     * @param message the error message
     * @param cause the cause of the exception
     */
    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
