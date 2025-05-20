package com.github.jshook.script;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.connection.QueryExecutionException;
import com.github.jshook.output.ResultFormatter;
import com.github.jshook.output.ResultFormatterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Executes CQL scripts from files or direct statements.
 */
public class ScriptExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ScriptExecutor.class);
    
    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;
    private final ResultFormatterFactory formatterFactory;
    private final PrintStream outputStream;
    private final PrintStream errorStream;
    
    // Special commands that require special handling
    private static final Pattern USE_KEYSPACE_PATTERN = Pattern.compile("^\\s*USE\\s+['\"]?([a-zA-Z0-9_]+)['\"]?\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);
    
    /**
     * Creates a new ScriptExecutor with the given connection manager and formatting configuration.
     *
     * @param connectionManager the connection manager to use for executing queries
     * @param formattingConfig  the formatting configuration to use for displaying results
     */
    public ScriptExecutor(ConnectionManager connectionManager, FormattingConfig formattingConfig) {
        this(connectionManager, formattingConfig, System.out, System.err);
    }
    
    /**
     * Creates a new ScriptExecutor with the given connection manager, formatting configuration,
     * and output streams.
     *
     * @param connectionManager the connection manager to use for executing queries
     * @param formattingConfig  the formatting configuration to use for displaying results
     * @param outputStream      the output stream to use for displaying results
     * @param errorStream       the error stream to use for displaying errors
     */
    public ScriptExecutor(ConnectionManager connectionManager, FormattingConfig formattingConfig,
                         PrintStream outputStream, PrintStream errorStream) {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
        this.formatterFactory = new ResultFormatterFactory(formattingConfig);
        this.outputStream = outputStream;
        this.errorStream = errorStream;
    }
    
    /**
     * Executes CQL statements from a file.
     *
     * @param file the file containing CQL statements
     * @return true if all statements executed successfully, false otherwise
     */
    public boolean executeFile(File file) {
        if (!file.exists()) {
            errorStream.printf("Error: File not found: %s%n", file.getPath());
            return false;
        }
        
        if (!file.canRead()) {
            errorStream.printf("Error: Cannot read file: %s%n", file.getPath());
            return false;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            List<String> statements = parseStatementsFromFile(reader);
            return executeStatements(statements);
        } catch (IOException e) {
            errorStream.printf("Error reading file: %s - %s%n", file.getPath(), e.getMessage());
            logger.error("Error reading script file", e);
            return false;
        }
    }
    
    /**
     * Parses CQL statements from a file.
     *
     * @param reader the reader for the file
     * @return a list of CQL statements
     * @throws IOException if an I/O error occurs
     */
    private List<String> parseStatementsFromFile(BufferedReader reader) throws IOException {
        List<String> statements = new ArrayList<>();
        StringBuilder currentStatement = new StringBuilder();
        String line;
        
        while ((line = reader.readLine()) != null) {
            // Skip empty lines and comments
            line = line.trim();
            if (line.isEmpty() || line.startsWith("--") || line.startsWith("//")) {
                continue;
            }
            
            currentStatement.append(line).append(" ");
            
            // If the line ends with a semicolon, it's the end of a statement
            if (line.endsWith(";")) {
                statements.add(currentStatement.toString().trim());
                currentStatement = new StringBuilder();
            }
        }
        
        // Add any remaining statement
        String remaining = currentStatement.toString().trim();
        if (!remaining.isEmpty()) {
            statements.add(remaining);
        }
        
        return statements;
    }
    
    /**
     * Executes a single CQL statement.
     *
     * @param statement the CQL statement to execute
     * @return true if the statement executed successfully, false otherwise
     */
    public boolean executeStatement(String statement) {
        List<String> statements = new ArrayList<>();
        statements.add(statement);
        return executeStatements(statements);
    }
    
    /**
     * Executes a list of CQL statements.
     *
     * @param statements the CQL statements to execute
     * @return true if all statements executed successfully, false otherwise
     */
    private boolean executeStatements(List<String> statements) {
        boolean success = true;
        
        for (String statement : statements) {
            // Skip empty statements
            if (statement.trim().isEmpty()) {
                continue;
            }
            
            try {
                // Handle special commands
                if (isSpecialCommand(statement)) {
                    handleSpecialCommand(statement);
                } else {
                    // Execute regular CQL statement
                    long startTime = System.currentTimeMillis();
                    ResultSet resultSet = connectionManager.execute(statement);
                    long endTime = System.currentTimeMillis();
                    
                    // Format and display the results
                    ResultFormatter formatter = formatterFactory.createFormatter(resultSet);
                    String formattedResult = formatter.format(resultSet);
                    
                    if (!formattedResult.isEmpty()) {
                        outputStream.println(formattedResult);
                    }
                    
                    // Show execution time if applicable
                    if (resultSet.getExecutionInfo() != null) {
                        outputStream.printf("(%d rows in %.3f sec)%n", 
                            resultSet.getAvailableWithoutFetching(), 
                            (endTime - startTime) / 1000.0);
                    }
                }
            } catch (QueryExecutionException e) {
                errorStream.println("ERROR: " + e.getMessage());
                logger.error("Query execution error", e);
                success = false;
                
                // Continue executing remaining statements
            } catch (Exception e) {
                errorStream.println("ERROR: " + e.getMessage());
                logger.error("Unexpected error during script execution", e);
                success = false;
            }
        }
        
        return success;
    }
    
    /**
     * Determines if a statement is a special command.
     *
     * @param statement the statement to check
     * @return true if the statement is a special command, false otherwise
     */
    private boolean isSpecialCommand(String statement) {
        // Currently, we only handle USE commands specially
        return USE_KEYSPACE_PATTERN.matcher(statement).matches();
    }
    
    /**
     * Handles a special command.
     *
     * @param statement the special command to handle
     * @throws QueryExecutionException if the command execution fails
     */
    private void handleSpecialCommand(String statement) throws QueryExecutionException {
        if (USE_KEYSPACE_PATTERN.matcher(statement).matches()) {
            // Extract keyspace name and use it
            var matcher = USE_KEYSPACE_PATTERN.matcher(statement);
            if (matcher.find()) {
                String keyspaceName = matcher.group(1).trim();
                connectionManager.useKeyspace(keyspaceName);
                outputStream.printf("Now using keyspace %s%n", keyspaceName);
            }
        } else {
            // For other commands, execute them directly
            connectionManager.execute(statement);
        }
    }
}
