package org.cqlsh.config;

import java.time.Duration;

/**
 * Immutable record representing Cassandra connection configuration.
 */
public record ConnectionConfig(
    String host,
    int port,
    String username,
    String password,
    String keyspace,
    Duration connectTimeout,
    Duration requestTimeout,
    boolean useSsl,
    String sslTruststorePath,
    String sslTruststorePassword,
    String localDatacenter
) {
    /**
     * Validates the connection configuration.
     * @throws IllegalArgumentException if any parameter is invalid
     */
    public ConnectionConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or blank");
        }

        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }

        if (connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("Connect timeout must be positive");
        }

        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("Request timeout must be positive");
        }

        if (useSsl && (sslTruststorePath == null || sslTruststorePath.isBlank())) {
            throw new IllegalArgumentException("SSL truststore path is required when SSL is enabled");
        }
    }
}
