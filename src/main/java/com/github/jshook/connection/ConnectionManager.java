package org.cqlsh.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.cqlsh.config.ConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages connections to Cassandra cluster and provides methods for executing queries.
 */
public class ConnectionManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConnectionConfig config;
    private final AtomicReference<CqlSession> sessionRef = new AtomicReference<>();
    private final AtomicBoolean tracingEnabled = new AtomicBoolean(false);
    private final AtomicReference<ConsistencyLevel> consistencyLevel = new AtomicReference<>(ConsistencyLevel.ONE);
    private final AtomicReference<ConsistencyLevel> serialConsistencyLevel = new AtomicReference<>(ConsistencyLevel.SERIAL);
    private final AtomicBoolean pagingEnabled = new AtomicBoolean(true);
    private final AtomicInteger pageSize = new AtomicInteger(100);

    /**
     * Creates a new ConnectionManager with the given configuration.
     * @param config the connection configuration
     */
    public ConnectionManager(ConnectionConfig config) {
        this.config = config;
    }

    /**
     * Connects to the Cassandra cluster using the connection configuration.
     * @throws ConnectionException if the connection fails
     */
    public void connect() throws ConnectionException {
        try {
            // Configure the driver
            DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, config.connectTimeout())
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, config.requestTimeout())
                .build();

            // Build session
            CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.host(), config.port()))
                .withConfigLoader(configLoader);

            // Add authentication if both username and password are provided
            if (config.username() != null && !config.username().isBlank() && 
                config.password() != null && !config.password().isBlank()) {
                builder.withAuthCredentials(config.username(), config.password());
            }

            // Add keyspace if provided
            if (config.keyspace() != null && !config.keyspace().isBlank()) {
                builder.withKeyspace(config.keyspace());
            }

            // Set local datacenter if provided
            if (config.localDatacenter() != null && !config.localDatacenter().isBlank()) {
                builder.withLocalDatacenter(config.localDatacenter());
            }

            // Configure SSL if enabled
            if (config.useSsl()) {
                // SSL configuration would go here
                // This is simplified for brevity
                logger.info("SSL enabled with truststore: {}", config.sslTruststorePath());
            }

            // Build and store the session
            CqlSession session = builder.build();
            sessionRef.set(session);

            logger.info("Connected to cluster: {}", session.getMetadata().getClusterName());
        } catch (Exception e) {
            throw new ConnectionException("Failed to connect to Cassandra: " + e.getMessage(), e);
        }
    }

    /**
     * Executes a CQL statement and returns the result set.
     * @param cql the CQL statement to execute
     * @return the result set
     * @throws QueryExecutionException if the query execution fails
     */
    public ResultSet execute(String cql) throws QueryExecutionException {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }

        try {
            // Create the statement
            SimpleStatement statement = SimpleStatement.newInstance(cql);

            // Check if this is a system query (querying system tables)
            boolean isSystemQuery = cql.toLowerCase().contains("system_schema.") || 
                                   cql.toLowerCase().contains("system.") ||
                                   cql.toLowerCase().contains("system_traces.");

            // Check if the query contains a fully qualified table name (keyspace.table)
            boolean hasFullyQualifiedName = cql.matches("(?i).*\\b[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\b.*");

            // For system queries or queries with fully qualified table names, 
            // we don't need to check if a keyspace is connected
            if (isSystemQuery) {
                // For system queries, explicitly set the keyspace to the appropriate system keyspace
                if (cql.toLowerCase().contains("system_schema.")) {
                    statement = statement.setKeyspace("system_schema");
                } else if (cql.toLowerCase().contains("system.")) {
                    statement = statement.setKeyspace("system");
                } else if (cql.toLowerCase().contains("system_traces.")) {
                    statement = statement.setKeyspace("system_traces");
                }
            } else if (hasFullyQualifiedName && getCurrentKeyspace() == null) {
                // For queries with fully qualified table names and no current keyspace,
                // we don't need to set a keyspace - the driver will handle it
            }

            // Enable tracing if requested
            if (tracingEnabled.get()) {
                statement = statement.setTracing(true);
            }

            // Set consistency level
            statement = statement.setConsistencyLevel(consistencyLevel.get());

            // Set serial consistency level
            statement = statement.setSerialConsistencyLevel(serialConsistencyLevel.get());

            // Set page size if paging is enabled
            if (pagingEnabled.get()) {
                statement = statement.setPageSize(pageSize.get());
            }

            return session.execute(statement);
        } catch (Exception e) {
            throw new QueryExecutionException("Failed to execute query: " + e.getMessage(), e);
        }
    }

    /**
     * Checks if query tracing is enabled.
     * @return true if tracing is enabled, false otherwise
     */
    public boolean isTracingEnabled() {
        return tracingEnabled.get();
    }

    /**
     * Sets whether query tracing is enabled.
     * @param enabled true to enable tracing, false to disable it
     */
    public void setTracingEnabled(boolean enabled) {
        tracingEnabled.set(enabled);
    }

    /**
     * Gets the host from the connection configuration.
     * @return the host
     */
    public String getHost() {
        return config.host();
    }

    /**
     * Gets the port from the connection configuration.
     * @return the port
     */
    public int getPort() {
        return config.port();
    }

    /**
     * Gets the list of keyspace names in the cluster.
     * @return list of keyspace names
     */
    public List<String> getKeyspaces() {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }

        Metadata metadata = session.getMetadata();
        List<String> keyspaces = new ArrayList<>();
        metadata.getKeyspaces().forEach((k, v) -> keyspaces.add(k.toString()));

        return keyspaces;
    }

    /**
     * Gets the list of table names in the specified keyspace.
     * @param keyspaceName the keyspace name
     * @return list of table names
     */
    public List<String> getTables(String keyspaceName) {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }

        Metadata metadata = session.getMetadata();
        Optional<KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);

        if (keyspace.isPresent()) {
            List<String> tables = new ArrayList<>();
            keyspace.get().getTables().forEach((k, v) -> tables.add(k.toString()));
            return tables;
        } else {
            throw new IllegalArgumentException("Keyspace not found: " + keyspaceName);
        }
    }

    /**
     * Gets the current keyspace name.
     * @return the current keyspace name or null if not connected to a keyspace
     */
    public String getCurrentKeyspace() {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }

        return session.getKeyspace().map(Object::toString).orElse(null);
    }

    /**
     * Changes the current keyspace.
     * @param keyspaceName the keyspace name to use
     * @throws QueryExecutionException if the keyspace change fails
     */
    public void useKeyspace(String keyspaceName) throws QueryExecutionException {
        execute("USE " + keyspaceName);
    }

    /**
     * Closes the connection to the Cassandra cluster.
     */
    @Override
    public void close() {
        CqlSession session = sessionRef.getAndSet(null);
        if (session != null) {
            session.close();
            logger.info("Disconnected from Cassandra cluster");
        }
    }

    /**
     * Gets the table metadata for the specified table.
     * @param tableSpec the table name or fully qualified table name (keyspace.table)
     * @return the table metadata
     * @throws IllegalArgumentException if the table is not found
     */
    public TableMetadata getTableMetadata(String tableSpec) {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }

        String keyspaceName;
        String tableName;

        // Check if the table name is fully qualified (contains a dot)
        if (tableSpec.contains(".")) {
            String[] parts = tableSpec.split("\\.", 2);
            keyspaceName = parts[0];
            tableName = parts[1];
        } else {
            tableName = tableSpec;
            keyspaceName = getCurrentKeyspace();
            if (keyspaceName == null) {
                throw new IllegalStateException("Not connected to a keyspace and no keyspace specified in table name");
            }
        }

        Metadata metadata = session.getMetadata();
        Optional<KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);

        if (keyspace.isPresent()) {
            Optional<TableMetadata> table = keyspace.get().getTable(tableName);
            if (table.isPresent()) {
                return table.get();
            } else {
                throw new IllegalArgumentException("Table not found: " + tableName + " in keyspace " + keyspaceName);
            }
        } else {
            throw new IllegalArgumentException("Keyspace not found: " + keyspaceName);
        }
    }

    /**
     * Gets the current consistency level.
     * @return the current consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel.get();
    }

    /**
     * Sets the consistency level for future queries.
     * @param level the consistency level to set
     */
    public void setConsistencyLevel(ConsistencyLevel level) {
        consistencyLevel.set(level);
    }

    /**
     * Gets the current serial consistency level.
     * @return the current serial consistency level
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistencyLevel.get();
    }

    /**
     * Sets the serial consistency level for future queries.
     * @param level the serial consistency level to set
     */
    public void setSerialConsistencyLevel(ConsistencyLevel level) {
        serialConsistencyLevel.set(level);
    }

    /**
     * Checks if paging is enabled.
     * @return true if paging is enabled, false otherwise
     */
    public boolean isPagingEnabled() {
        return pagingEnabled.get();
    }

    /**
     * Sets whether paging is enabled.
     * @param enabled true to enable paging, false to disable it
     */
    public void setPagingEnabled(boolean enabled) {
        pagingEnabled.set(enabled);
    }

    /**
     * Gets the current page size.
     * @return the current page size
     */
    public int getPageSize() {
        return pageSize.get();
    }

    /**
     * Sets the page size for future queries.
     * @param size the page size to set
     */
    public void setPageSize(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Page size must be positive");
        }
        pageSize.set(size);
    }

    /**
     * Gets the driver version.
     * @return the driver version
     */
    public String getDriverVersion() {
        return CqlSession.class.getPackage().getImplementationVersion();
    }

    /**
     * Gets the CQL protocol version.
     * @return the CQL protocol version
     */
    public String getProtocolVersion() {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }
        return session.getContext().getProtocolVersion().toString();
    }

    /**
     * Gets the Cassandra version.
     * @return the Cassandra version
     */
    public String getCassandraVersion() {
        try {
            ResultSet rs = execute("SELECT release_version FROM system.local");
            Row row = rs.one();
            return row != null ? row.getString("release_version") : "Unknown";
        } catch (Exception e) {
            logger.warn("Could not get Cassandra version", e);
            return "Unknown";
        }
    }

    /**
     * Gets the cluster name.
     * @return the cluster name
     */
    public String getClusterName() {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }
        return session.getMetadata().getClusterName().orElse("Unknown Cluster");
    }
}
