package com.github.jshook.shell;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.connection.QueryExecutionException;
import com.github.jshook.output.ResultFormatter;
import com.github.jshook.output.ResultFormatterFactory;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Interactive shell implementation for the CQL shell, providing command history,
 * tab completion, and other interactive features.
 */
public class InteractiveShell {
    private static final Logger logger = LoggerFactory.getLogger(InteractiveShell.class);
    
    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;
    private final LineReader lineReader;
    private final Terminal terminal;
    private final ResultFormatterFactory formatterFactory;
    private final CommandRegistry commandRegistry;
    
    // CQL Keywords for auto-completion
    private static final String[] CQL_KEYWORDS = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
        "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "KEYSPACE", "TYPE", "INDEX", "MATERIALIZED",
        "VIEW", "USE", "WITH", "AS", "ORDER", "BY", "ASC", "DESC", "LIMIT", "ALLOW", "FILTERING",
        "CONTAINS", "USING", "TTL", "TIMESTAMP", "IF", "NOT", "EXISTS", "PRIMARY", "KEY", "FROZEN",
        "LIST", "MAP", "SET", "TUPLE", "FUNCTION", "RETURNS", "CALLED", "INPUT", "LANGUAGE", "AGGREGATE",
        "SFUNC", "STYPE", "FINALFUNC", "INITCOND", "ROLE", "AUTHORIZE", "GRANT", "REVOKE", "PERMISSION",
        "ON", "TO", "OF", "MODIFY", "DESCRIBE", "TEXT", "ASCII", "BIGINT", "BLOB", "BOOLEAN", "COUNTER",
        "DATE", "DECIMAL", "DOUBLE", "FLOAT", "INET", "INT", "SMALLINT", "TIME", "TIMESTAMP", "TIMEUUID",
        "TINYINT", "UUID", "VARCHAR", "VARINT", "TOKEN", "WRITETIME", "NULL", "TRUE", "FALSE"
    };

    /**
     * Creates a new InteractiveShell with the given connection manager and formatting configuration.
     *
     * @param connectionManager the connection manager to use for executing queries
     * @param formattingConfig  the formatting configuration to use for displaying results
     * @throws IOException if the terminal cannot be created
     */
    public InteractiveShell(ConnectionManager connectionManager, FormattingConfig formattingConfig) throws IOException {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
        this.formatterFactory = new ResultFormatterFactory(formattingConfig);
        this.commandRegistry = new CommandRegistry();
        
        // Register special commands
        registerSpecialCommands();

        // Set up the terminal
        this.terminal = TerminalBuilder.builder()
                .name("JCqlsh")
                .system(true)
                .jansi(true)
                .build();
        
        // Configure the history file
        String userHome = System.getProperty("user.home");
        Path historyFile = Paths.get(userHome, ".jcqlsh_history");
        if (!Files.exists(historyFile)) {
            try {
                Files.createFile(historyFile);
            } catch (IOException e) {
                logger.warn("Could not create history file: {}", e.getMessage());
            }
        }
        
        // Create the completer for auto-completion
        Completer completer = createCompleter();
        
        // Set up the line reader with history and auto-completion
        DefaultParser parser = new DefaultParser();
        parser.setEscapeChars(null);
        
        this.lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(completer)
                .parser(parser)
                .variable(LineReader.HISTORY_FILE, historyFile)
                .variable(LineReader.HISTORY_FILE_SIZE, 500)
                .history(new DefaultHistory())
                .option(LineReader.Option.AUTO_FRESH_LINE, true)
                .option(LineReader.Option.AUTO_GROUP, true)
                .option(LineReader.Option.CASE_INSENSITIVE, true)
                .option(LineReader.Option.INSERT_TAB, true)
                .build();
    }

    /**
     * Starts the interactive shell, displaying a welcome message and entering the command loop.
     *
     * @return 0 if the shell exits normally, non-zero otherwise
     */
    public int start() {
        // Display welcome message
        printWelcome();
        
        boolean running = true;
        while (running) {
            // Get current keyspace for the prompt
            String currentKeyspace = connectionManager.getCurrentKeyspace();
            String prompt = (currentKeyspace == null) ? "jcqlsh> " : "jcqlsh:" + currentKeyspace + "> ";
            
            // Read line from terminal with proper prompt
            String line;
            try {
                line = lineReader.readLine(prompt);
                
                // Handle the input
                if (line == null) {
                    // EOF - exit the shell
                    running = false;
                    continue;
                }
                
                // Trim the input
                line = line.trim();
                
                // Skip empty lines
                if (line.isEmpty()) {
                    continue;
                }
                
                // Handle special commands
                if (isSpecialCommand(line)) {
                    boolean shouldContinue = handleSpecialCommand(line);
                    if (!shouldContinue) {
                        running = false;
                    }
                } else {
                    // Execute CQL statement
                    executeCql(line);
                }
            } catch (UserInterruptException e) {
                // User pressed Ctrl-C
                terminal.writer().println("^C");
            } catch (EndOfFileException e) {
                // User pressed Ctrl-D
                running = false;
            } catch (Exception e) {
                terminal.writer().println("Error: " + e.getMessage());
                logger.error("Error in command loop", e);
            }
        }
        
        // Goodbye message
        terminal.writer().println("Goodbye!");
        return 0;
    }

    /**
     * Creates a completer for auto-completion of CQL keywords, keyspaces, tables, and columns.
     *
     * @return a completer for auto-completion
     */
    private Completer createCompleter() {
        // Create completers for CQL keywords
        Completer keywordCompleter = new StringsCompleter(CQL_KEYWORDS);
        
        // Create a dynamic completer for keyspaces
        Completer keyspaceCompleter = (LineReader reader, ParsedLine line, List<Candidate> candidates) -> {
            try {
                List<String> keyspaces = connectionManager.getKeyspaces();
                keyspaces.forEach(keyspace -> candidates.add(new Candidate(keyspace)));
            } catch (Exception e) {
                logger.warn("Error retrieving keyspaces for completion", e);
            }
        };
        
        // Create a dynamic completer for tables in the current keyspace
        Completer tableCompleter = (LineReader reader, ParsedLine line, List<Candidate> candidates) -> {
            try {
                String currentKeyspace = connectionManager.getCurrentKeyspace();
                if (currentKeyspace != null) {
                    List<String> tables = connectionManager.getTables(currentKeyspace);
                    tables.forEach(table -> candidates.add(new Candidate(table)));
                }
            } catch (Exception e) {
                logger.warn("Error retrieving tables for completion", e);
            }
        };
        
        // Create a completer for special commands
        Completer specialCommandCompleter = new StringsCompleter(
                "HELP", "QUIT", "EXIT", "CLEAR", "DESCRIBE", "DESC", "USE",
                "COPY", "SOURCE", "CAPTURE", "TRACING", "EXPAND", "CONSISTENCY"
        );
        
        // Combine all completers
        return new AggregateCompleter(
                keywordCompleter,
                keyspaceCompleter,
                tableCompleter,
                specialCommandCompleter
        );
    }

    /**
     * Registers special commands with the command registry.
     */
    private void registerSpecialCommands() {
        // Basic shell commands
        commandRegistry.register("HELP", this::handleHelp);
        commandRegistry.register("EXIT", args -> false);
        commandRegistry.register("QUIT", args -> false);
        commandRegistry.register("CLEAR", this::handleClear);
        
        // Database and schema commands
        commandRegistry.register("USE", this::handleUse);
        commandRegistry.register("DESCRIBE", this::handleDescribe);
        commandRegistry.register("DESC", this::handleDescribe);
        
        // Output formatting commands
        commandRegistry.register("EXPAND", this::handleExpand);
        commandRegistry.register("TRACING", this::handleTracing);
        
        // File operations
        commandRegistry.register("SOURCE", this::handleSource);
        commandRegistry.register("CAPTURE", this::handleCapture);
    }

    /**
     * Determines if the input line is a special command.
     *
     * @param line the input line
     * @return true if the input line is a special command, false otherwise
     */
    private boolean isSpecialCommand(String line) {
        String command = line.split("\\s+")[0].toUpperCase();
        return commandRegistry.isRegistered(command);
    }

    /**
     * Handles a special command.
     *
     * @param line the input line containing the special command
     * @return true if the shell should continue running, false if it should exit
     */
    private boolean handleSpecialCommand(String line) {
        String[] parts = line.split("\\s+", 2);
        String command = parts[0].toUpperCase();
        String args = parts.length > 1 ? parts[1] : "";
        
        return commandRegistry.execute(command, args);
    }

    /**
     * Executes a CQL statement.
     *
     * @param cql the CQL statement to execute
     */
    private void executeCql(String cql) {
        try {
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = connectionManager.execute(cql);
            long endTime = System.currentTimeMillis();
            
            // Format and display the results
            ResultFormatter formatter = formatterFactory.createFormatter(resultSet);
            String formattedResult = formatter.format(resultSet);
            
            terminal.writer().println(formattedResult);
            
            // Show execution time if applicable
            if (resultSet.getExecutionInfo() != null) {
                terminal.writer().printf("(%d rows in %.3f sec)%n", 
                    resultSet.getAvailableWithoutFetching(), 
                    (endTime - startTime) / 1000.0);
            }
        } catch (QueryExecutionException e) {
            terminal.writer().println("ERROR: " + e.getMessage());
            logger.error("Query execution error", e);
        }
    }

    /**
     * Prints a welcome message for the shell.
     */
    private void printWelcome() {
        terminal.writer().println("Java CQL Shell (JCqlsh) v1.0.0");
        terminal.writer().println("Type 'help' for help, 'quit' to exit.");
        
        // Display connection information
        try {
            String clusterName = connectionManager.execute("SELECT cluster_name FROM system.local").one().getString("cluster_name");
            terminal.writer().printf("Connected to %s at %s:%d%n", 
                    clusterName, 
                    connectionManager.getHost(), 
                    connectionManager.getPort());
        } catch (Exception e) {
            logger.warn("Could not get cluster name", e);
        }
        
        terminal.writer().println();
    }

    // Command handlers

    /**
     * Handles the HELP command.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleHelp(String args) {
        if (args.isEmpty()) {
            terminal.writer().println("Available commands:");
            terminal.writer().println();
            terminal.writer().println("HELP               - Show this help message");
            terminal.writer().println("EXIT, QUIT         - Exit the shell");
            terminal.writer().println("CLEAR              - Clear the screen");
            terminal.writer().println("USE <keyspace>     - Switch to a keyspace");
            terminal.writer().println("DESCRIBE [object]  - Describe a keyspace, table, or other object");
            terminal.writer().println("EXPAND ON|OFF      - Toggle expanded (vertical) output");
            terminal.writer().println("TRACING ON|OFF     - Toggle query tracing");
            terminal.writer().println("SOURCE <file>      - Execute commands from a file");
            terminal.writer().println("CAPTURE <file>     - Begin saving output to a file");
            terminal.writer().println();
            terminal.writer().println("For help on CQL, type 'HELP CQL'");
        } else if (args.equalsIgnoreCase("CQL")) {
            terminal.writer().println("CQL (Cassandra Query Language) Quick Reference:");
            terminal.writer().println();
            terminal.writer().println("Data Definition:");
            terminal.writer().println("  CREATE KEYSPACE <name> WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': N }");
            terminal.writer().println("  CREATE TABLE <name> (<column> <type> PRIMARY KEY, <column> <type>, ...)");
            terminal.writer().println("  ALTER TABLE <name> ADD <column> <type>");
            terminal.writer().println("  DROP TABLE <name>");
            terminal.writer().println("  DROP KEYSPACE <name>");
            terminal.writer().println();
            terminal.writer().println("Data Manipulation:");
            terminal.writer().println("  INSERT INTO <table> (<columns>) VALUES (<values>)");
            terminal.writer().println("  UPDATE <table> SET <assignments> WHERE <conditions>");
            terminal.writer().println("  DELETE FROM <table> WHERE <conditions>");
            terminal.writer().println("  SELECT <columns> FROM <table> WHERE <conditions>");
        } else {
            terminal.writer().printf("No help available for: %s%n", args);
        }
        return true;
    }

    /**
     * Handles the CLEAR command.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleClear(String args) {
        terminal.writer().print("\033[H\033[2J");
        terminal.writer().flush();
        return true;
    }

    /**
     * Handles the USE command to switch keyspaces.
     *
     * @param keyspaceName the keyspace name to use
     * @return true to continue running the shell
     */
    private boolean handleUse(String keyspaceName) {
        if (keyspaceName.isEmpty()) {
            terminal.writer().println("ERROR: Keyspace name required");
            return true;
        }
        
        try {
            connectionManager.useKeyspace(keyspaceName.trim());
            terminal.writer().printf("Now using keyspace %s%n", keyspaceName.trim());
        } catch (QueryExecutionException e) {
            terminal.writer().println("ERROR: " + e.getMessage());
            logger.error("Keyspace change error", e);
        }
        return true;
    }

    /**
     * Handles the DESCRIBE command to describe schema objects.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleDescribe(String args) {
        if (args.isEmpty() || args.equalsIgnoreCase("SCHEMA")) {
            // Describe all keyspaces
            try {
                List<String> keyspaces = connectionManager.getKeyspaces();
                terminal.writer().println("Keyspaces:");
                for (String keyspace : keyspaces) {
                    terminal.writer().println("  " + keyspace);
                }
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error describing schema", e);
            }
        } else if (args.toUpperCase().startsWith("KEYSPACE") || args.toUpperCase().startsWith("KEYSPACES")) {
            String[] parts = args.split("\\s+", 2);
            String keyspaceName = parts.length > 1 ? parts[1] : connectionManager.getCurrentKeyspace();
            
            if (keyspaceName == null) {
                terminal.writer().println("ERROR: No keyspace specified and not connected to a keyspace");
                return true;
            }
            
            // Describe the specified keyspace
            try {
                ResultSet results = connectionManager.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspaceName + "'");
                if (results.one() == null) {
                    terminal.writer().printf("ERROR: Keyspace '%s' does not exist%n", keyspaceName);
                    return true;
                }
                
                ResultSet tableResults = connectionManager.execute(
                        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + keyspaceName + "'");
                
                terminal.writer().printf("Keyspace %s%n", keyspaceName);
                terminal.writer().println("Tables:");
                tableResults.forEach(row -> {
                    terminal.writer().println("  " + row.getString("table_name"));
                });
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error describing keyspace", e);
            }
        } else if (args.toUpperCase().startsWith("TABLE") || args.toUpperCase().startsWith("TABLES")) {
            String[] parts = args.split("\\s+", 2);
            if (parts.length < 2) {
                terminal.writer().println("ERROR: Table name required");
                return true;
            }
            
            String tableName = parts[1];
            String keyspaceName = connectionManager.getCurrentKeyspace();
            
            if (keyspaceName == null) {
                terminal.writer().println("ERROR: Not connected to a keyspace");
                return true;
            }
            
            // Describe the specified table
            try {
                // Get table schema
                TableMetadata tableMetadata = connectionManager.getTableMetadata(tableName);
                if (tableMetadata == null) {
                    terminal.writer().printf("ERROR: Table '%s' does not exist%n", tableName);
                    return true;
                }
                
                terminal.writer().printf("Table %s.%s%n", keyspaceName, tableName);
                
                // Display columns and their types
                terminal.writer().println("Columns:");
                tableMetadata.getColumns().forEach((name, column) -> {
                    terminal.writer().printf("  %-20s %s%n", name, column.getType());
                });
                
                // Display primary key
                terminal.writer().println("Primary Key:");
                terminal.writer().println("  " + tableMetadata.getPrimaryKey().stream()
                        .map(col -> col.getName().asInternal())
                        .collect(Collectors.joining(", ")));
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error describing table", e);
            }
        } else {
            terminal.writer().printf("ERROR: Cannot describe '%s'. Use DESCRIBE KEYSPACE or DESCRIBE TABLE.%n", args);
        }
        return true;
    }

    /**
     * Handles the EXPAND command to toggle expanded (vertical) output.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleExpand(String args) {
        if (args.isEmpty()) {
            boolean expanded = formattingConfig.isExpandedFormat();
            terminal.writer().printf("Expanded display is %s.%n", expanded ? "on" : "off");
        } else if (args.equalsIgnoreCase("ON")) {
            formattingConfig.setExpandedFormat(true);
            terminal.writer().println("Expanded display is now on.");
        } else if (args.equalsIgnoreCase("OFF")) {
            formattingConfig.setExpandedFormat(false);
            terminal.writer().println("Expanded display is now off.");
        } else {
            terminal.writer().println("ERROR: Invalid argument. Use EXPAND ON or EXPAND OFF.");
        }
        return true;
    }

    /**
     * Handles the TRACING command to toggle query tracing.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleTracing(String args) {
        if (args.isEmpty()) {
            boolean tracing = connectionManager.isTracingEnabled();
            terminal.writer().printf("Tracing is %s.%n", tracing ? "on" : "off");
        } else if (args.equalsIgnoreCase("ON")) {
            connectionManager.setTracingEnabled(true);
            terminal.writer().println("Tracing is now on.");
        } else if (args.equalsIgnoreCase("OFF")) {
            connectionManager.setTracingEnabled(false);
            terminal.writer().println("Tracing is now off.");
        } else {
            terminal.writer().println("ERROR: Invalid argument. Use TRACING ON or TRACING OFF.");
        }
        return true;
    }

    /**
     * Handles the SOURCE command to execute commands from a file.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleSource(String args) {
        if (args.isEmpty()) {
            terminal.writer().println("ERROR: File name required");
            return true;
        }
        
        File file = new File(args.trim());
        if (!file.exists() || !file.isFile() || !file.canRead()) {
            terminal.writer().printf("ERROR: Cannot read file '%s'%n", args);
            return true;
        }
        
        try {
            terminal.writer().printf("Executing commands from '%s'%n", file.getPath());
            
            List<String> lines = Files.readAllLines(file.toPath());
            StringBuilder commandBuilder = new StringBuilder();
            
            for (String line : lines) {
                line = line.trim();
                
                // Skip empty lines and comments
                if (line.isEmpty() || line.startsWith("--") || line.startsWith("//")) {
                    continue;
                }
                
                commandBuilder.append(line).append(" ");
                
                // If the line ends with a semicolon, execute the statement
                if (line.endsWith(";")) {
                    String command = commandBuilder.toString().trim();
                    terminal.writer().printf("> %s%n", command);
                    
                    if (isSpecialCommand(command)) {
                        handleSpecialCommand(command);
                    } else {
                        executeCql(command);
                    }
                    
                    commandBuilder = new StringBuilder();
                }
            }
            
            // Execute any remaining command
            String remainingCommand = commandBuilder.toString().trim();
            if (!remainingCommand.isEmpty()) {
                terminal.writer().printf("> %s%n", remainingCommand);
                
                if (isSpecialCommand(remainingCommand)) {
                    handleSpecialCommand(remainingCommand);
                } else {
                    executeCql(remainingCommand);
                }
            }
            
            terminal.writer().println("File execution completed.");
        } catch (IOException e) {
            terminal.writer().printf("ERROR: Could not read file '%s': %s%n", file.getPath(), e.getMessage());
            logger.error("File read error", e);
        } catch (Exception e) {
            terminal.writer().printf("ERROR: Error executing file '%s': %s%n", file.getPath(), e.getMessage());
            logger.error("File execution error", e);
        }
        
        return true;
    }

    /**
     * Handles the CAPTURE command to begin saving output to a file.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleCapture(String args) {
        terminal.writer().println("CAPTURE command not implemented yet.");
        return true;
    }

    /**
     * Command registry to handle special commands.
     */
    private static class CommandRegistry {
        // Function interface for command handlers
        @FunctionalInterface
        interface CommandHandler {
            boolean handle(String args);
        }
        
        // Map of commands to their handlers
        private final Map<String, CommandHandler> commands = new HashMap<>();
        
        /**
         * Registers a command with a handler.
         *
         * @param command the command name
         * @param handler the handler for the command
         */
        public void register(String command, CommandHandler handler) {
            commands.put(command.toUpperCase(), handler);
        }
        
        /**
         * Checks if a command is registered.
         *
         * @param command the command name
         * @return true if the command is registered, false otherwise
         */
        public boolean isRegistered(String command) {
            return commands.containsKey(command.toUpperCase());
        }
        
        /**
         * Executes a command with the given arguments.
         *
         * @param command the command name
         * @param args    the arguments to the command
         * @return true if the shell should continue running, false if it should exit
         */
        public boolean execute(String command, String args) {
            CommandHandler handler = commands.get(command.toUpperCase());
            if (handler != null) {
                return handler.handle(args);
            }
            return true;
        }
    }
}
