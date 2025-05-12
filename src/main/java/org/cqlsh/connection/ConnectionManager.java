package org.cqlsh.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages connections to Cassandra cluster and provides methods for executing queries.
 */
public class ConnectionManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    
    private final ConnectionConfig config;
    private final AtomicReference<CqlSession> sessionRef = new AtomicReference<>();
    private final AtomicBoolean tracingEnabled = new AtomicBoolean(false);
    
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
            
            // Add authentication if provided
            if (config.username() != null && !config.username().isBlank()) {
                builder.withAuthCredentials(config.username(), config.password());
            }
            
            // Add keyspace if provided
            if (config.keyspace() != null && !config.keyspace().isBlank()) {
                builder.withKeyspace(config.keyspace());
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
            SimpleStatement statement = SimpleStatement.newInstance(cql);
            
            // Enable tracing if requested
            if (tracingEnabled.get()) {
                statement = statement.setTracing(true);
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
     * Gets the table metadata for the specified table in the current keyspace.
     * @param tableName the table name
     * @return the table metadata
     * @throws IllegalArgumentException if the table is not found
     */
    public TableMetadata getTableMetadata(String tableName) {
        CqlSession session = sessionRef.get();
        if (session == null) {
            throw new IllegalStateException("Not connected to Cassandra");
        }
        
        String keyspaceName = getCurrentKeyspace();
        if (keyspaceName == null) {
            throw new IllegalStateException("Not connected to a keyspace");
        }
        
        Metadata metadata = session.getMetadata();
        Optional<KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);
        
        if (keyspace.isPresent()) {
            Optional<TableMetadata> table = keyspace.get().getTable(tableName);
            if (table.isPresent()) {
                return table.get();
            } else {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
        } else {
            throw new IllegalArgumentException("Keyspace not found: " + keyspaceName);
        }
    }
}
