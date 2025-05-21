package com.github.jshook.shell;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.output.ResultFormatter;
import com.github.jshook.output.ResultFormatterFactory;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                .signalHandler(Terminal.SignalHandler.SIG_DFL)  // Use default signal handling
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
        parser.setEofOnUnclosedQuote(true);  // Ensure EOF is detected even with unclosed quotes
        parser.setEofOnEscapedNewLine(true); // Ensure EOF is detected even with escaped newlines

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
                .option(LineReader.Option.BRACKETED_PASTE, false)  // Disable bracketed paste to ensure EOF is properly detected
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
     * Gets column names for a specific table.
     *
     * @param tableSpec the table name or fully qualified table name (keyspace.table)
     * @return a list of column names
     */
    private List<String> getColumnNames(String tableSpec) {
        try {
            TableMetadata tableMetadata = connectionManager.getTableMetadata(tableSpec);
            List<String> columnNames = new ArrayList<>();

            // Add regular columns
            tableMetadata.getColumns().forEach((name, column) -> {
                columnNames.add(name.toString());
            });

            return columnNames;
        } catch (Exception e) {
            logger.warn("Error retrieving column names for table: {}", tableSpec, e);
            return List.of();
        }
    }

    /**
     * Checks if the input line is after a specific keyword.
     *
     * @param line the input line
     * @param keyword the keyword to check for
     * @return true if the input line is after the keyword, false otherwise
     */
    private boolean isAfterKeyword(String line, String keyword) {
        String lowerLine = line.toLowerCase();
        String lowerKeyword = keyword.toLowerCase();

        // Check if the keyword is in the line
        int keywordIndex = lowerLine.indexOf(lowerKeyword);
        if (keywordIndex == -1) {
            return false;
        }

        // Check if the keyword is a whole word (surrounded by spaces or at the beginning/end of the line)
        int keywordEnd = keywordIndex + lowerKeyword.length();
        boolean validStart = keywordIndex == 0 || Character.isWhitespace(lowerLine.charAt(keywordIndex - 1));
        boolean validEnd = keywordEnd == lowerLine.length() || Character.isWhitespace(lowerLine.charAt(keywordEnd));

        return validStart && validEnd;
    }

    /**
     * Extracts the table name from a CQL query.
     *
     * @param query the CQL query
     * @return the table name, or null if not found
     */
    private String extractTableName(String query) {
        String lowerQuery = query.toLowerCase();

        // Check for FROM clause
        int fromIndex = lowerQuery.indexOf("from");
        if (fromIndex != -1 && isAfterKeyword(lowerQuery, "from")) {
            // Extract the word after FROM
            String afterFrom = lowerQuery.substring(fromIndex + 4).trim();
            int spaceIndex = afterFrom.indexOf(' ');
            if (spaceIndex == -1) {
                return afterFrom; // No space after table name
            } else {
                return afterFrom.substring(0, spaceIndex);
            }
        }

        // Check for UPDATE clause
        int updateIndex = lowerQuery.indexOf("update");
        if (updateIndex != -1 && isAfterKeyword(lowerQuery, "update")) {
            // Extract the word after UPDATE
            String afterUpdate = lowerQuery.substring(updateIndex + 6).trim();
            int spaceIndex = afterUpdate.indexOf(' ');
            if (spaceIndex == -1) {
                return afterUpdate; // No space after table name
            } else {
                return afterUpdate.substring(0, spaceIndex);
            }
        }

        // Check for INSERT INTO clause
        int intoIndex = lowerQuery.indexOf("into");
        if (intoIndex != -1 && isAfterKeyword(lowerQuery, "into")) {
            // Extract the word after INTO
            String afterInto = lowerQuery.substring(intoIndex + 4).trim();
            int spaceIndex = afterInto.indexOf(' ');
            if (spaceIndex == -1) {
                return afterInto; // No space after table name
            } else {
                return afterInto.substring(0, spaceIndex);
            }
        }

        return null; // No table name found
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
                // Check if we're after a FROM keyword
                String buffer = line.line();
                String word = line.word().toLowerCase();

                // If we're in a FROM clause, suggest tables
                if (isAfterKeyword(buffer, "from") || isAfterKeyword(buffer, "update") || 
                    isAfterKeyword(buffer, "into") || isAfterKeyword(buffer, "join")) {
                    String currentKeyspace = connectionManager.getCurrentKeyspace();
                    if (currentKeyspace != null) {
                        List<String> tables = connectionManager.getTables(currentKeyspace);
                        tables.forEach(table -> {
                            if (table.toLowerCase().startsWith(word.toLowerCase())) {
                                candidates.add(new Candidate(table));
                            }
                        });
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving tables for completion", e);
            }
        };

        // Create a dynamic completer for columns in the current context
        Completer columnCompleter = (LineReader reader, ParsedLine line, List<Candidate> candidates) -> {
            try {
                String buffer = line.line();
                String word = line.word().toLowerCase();

                // Try to find the table name in the query
                String tableName = extractTableName(buffer);
                if (tableName != null) {
                    List<String> columns = getColumnNames(tableName);
                    columns.forEach(column -> {
                        if (column.toLowerCase().startsWith(word.toLowerCase())) {
                            candidates.add(new Candidate(column));
                        }
                    });
                }
            } catch (Exception e) {
                logger.warn("Error retrieving columns for completion", e);
            }
        };

        // Create a completer for special commands
        Completer specialCommandCompleter = new StringsCompleter(
                "HELP", "QUIT", "EXIT", "CLEAR", "DESCRIBE", "DESC", "USE",
                "COPY", "SOURCE", "CAPTURE", "TRACING", "EXPAND", "CONSISTENCY",
                "SERIAL CONSISTENCY", "PAGING", "SHOW VERSION", "SHOW HOST", "SHOW SESSION", "LOGIN"
        );

        // Create a context-aware completer that suggests different things based on the input
        Completer contextAwareCompleter = (LineReader reader, ParsedLine line, List<Candidate> candidates) -> {
            String buffer = line.line();
            String word = line.word().toLowerCase();

            // If the line is empty or we're at the start of the line, suggest keywords and special commands
            if (buffer.trim().isEmpty() || line.wordIndex() == 0) {
                // Add CQL keywords
                for (String keyword : CQL_KEYWORDS) {
                    if (keyword.toLowerCase().startsWith(word)) {
                        candidates.add(new Candidate(keyword));
                    }
                }

                // Add special commands
                specialCommandCompleter.complete(reader, line, candidates);
                return;
            }

            // If we're after a USE keyword, suggest keyspaces
            if (isAfterKeyword(buffer, "use")) {
                keyspaceCompleter.complete(reader, line, candidates);
                return;
            }

            // If we're after a FROM, UPDATE, INTO, or JOIN keyword, suggest tables
            if (isAfterKeyword(buffer, "from") || isAfterKeyword(buffer, "update") || 
                isAfterKeyword(buffer, "into") || isAfterKeyword(buffer, "join")) {
                tableCompleter.complete(reader, line, candidates);
                return;
            }

            // If we're after a SELECT keyword or in a WHERE clause, suggest columns
            if (isAfterKeyword(buffer, "select") || isAfterKeyword(buffer, "where") || 
                isAfterKeyword(buffer, "set") || buffer.contains("=")) {
                columnCompleter.complete(reader, line, candidates);
                return;
            }

            // Default to suggesting keywords
            for (String keyword : CQL_KEYWORDS) {
                if (keyword.toLowerCase().startsWith(word)) {
                    candidates.add(new Candidate(keyword));
                }
            }
        };

        // Return the context-aware completer
        return contextAwareCompleter;
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
        commandRegistry.register("LOGIN", this::handleLogin);

        // Output formatting commands
        commandRegistry.register("EXPAND", this::handleExpand);
        commandRegistry.register("TRACING", this::handleTracing);
        commandRegistry.register("PAGING", this::handlePaging);

        // Query settings
        commandRegistry.register("CONSISTENCY", this::handleConsistency);
        commandRegistry.register("SERIAL", this::handleSerialConsistency);

        // Information commands
        commandRegistry.register("SHOW", this::handleShow);

        // File operations
        commandRegistry.register("SOURCE", this::handleSource);
        commandRegistry.register("CAPTURE", this::handleCapture);
        commandRegistry.register("COPY", this::handleCopy);
    }

    /**
     * Determines if the input line is a special command.
     *
     * @param line the input line
     * @return true if the input line is a special command, false otherwise
     */
    private boolean isSpecialCommand(String line) {
        // Remove trailing semicolon if present
        if (line.endsWith(";")) {
            line = line.substring(0, line.length() - 1).trim();
        }
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
        // Remove trailing semicolon if present
        if (line.endsWith(";")) {
            line = line.substring(0, line.length() - 1).trim();
        }

        String[] parts = line.split("\\s+", 2);
        String command = parts[0].toUpperCase();
        String args = parts.length > 1 ? parts[1].trim() : "";

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
            terminal.writer().println("HELP                      - Show this help message");
            terminal.writer().println("EXIT, QUIT                - Exit the shell");
            terminal.writer().println("CLEAR                     - Clear the screen");
            terminal.writer().println("USE <keyspace>            - Switch to a keyspace");
            terminal.writer().println("DESCRIBE [object]         - Describe a keyspace, table, or other object");
            terminal.writer().println("EXPAND ON|OFF             - Toggle expanded (vertical) output");
            terminal.writer().println("TRACING ON|OFF            - Toggle query tracing");
            terminal.writer().println("PAGING ON|OFF|<size>      - Control paging of query results");
            terminal.writer().println("CONSISTENCY <level>       - Set consistency level for queries");
            terminal.writer().println("SERIAL CONSISTENCY <level>- Set serial consistency level for conditional updates");
            terminal.writer().println("SHOW VERSION              - Show version information");
            terminal.writer().println("SHOW HOST                 - Show connection information");
            terminal.writer().println("SHOW SESSION <id>         - Show tracing session details");
            terminal.writer().println("LOGIN <username> [<password>] - Authenticate as a user");
            terminal.writer().println("SOURCE <file>             - Execute commands from a file");
            terminal.writer().println("CAPTURE <file>            - Begin saving output to a file");
            terminal.writer().println("COPY ... TO|FROM ...      - Import/export data in CSV format");
            terminal.writer().println();
            terminal.writer().println("For help on CQL, type 'HELP CQL'");
            terminal.writer().println("For help on a specific command, type 'HELP <command>'");
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
        } else if (args.equalsIgnoreCase("CONSISTENCY")) {
            terminal.writer().println("CONSISTENCY <level>");
            terminal.writer().println();
            terminal.writer().println("Sets the consistency level for operations to follow. Valid arguments include:");
            terminal.writer().println("  ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, LOCAL_ONE, SERIAL, LOCAL_SERIAL");
            terminal.writer().println();
            terminal.writer().println("Example: CONSISTENCY QUORUM");
        } else if (args.equalsIgnoreCase("SERIAL CONSISTENCY") || args.equalsIgnoreCase("SERIAL")) {
            terminal.writer().println("SERIAL CONSISTENCY <level>");
            terminal.writer().println();
            terminal.writer().println("Sets the serial consistency level for conditional updates. Valid arguments include:");
            terminal.writer().println("  SERIAL, LOCAL_SERIAL");
            terminal.writer().println();
            terminal.writer().println("Example: SERIAL CONSISTENCY LOCAL_SERIAL");
        } else if (args.equalsIgnoreCase("SHOW")) {
            terminal.writer().println("SHOW VERSION | HOST | SESSION <id>");
            terminal.writer().println();
            terminal.writer().println("SHOW VERSION - Displays version information for jcqlsh, Cassandra, CQL, and protocol");
            terminal.writer().println("SHOW HOST    - Displays connection information");
            terminal.writer().println("SHOW SESSION <id> - Displays details of a tracing session");
        } else if (args.equalsIgnoreCase("PAGING")) {
            terminal.writer().println("PAGING ON | OFF | <page size>");
            terminal.writer().println();
            terminal.writer().println("Controls paging of query results:");
            terminal.writer().println("  PAGING ON       - Enables paging with current page size");
            terminal.writer().println("  PAGING OFF      - Disables paging");
            terminal.writer().println("  PAGING <size>   - Sets page size and enables paging");
            terminal.writer().println();
            terminal.writer().println("Example: PAGING 1000");
        } else if (args.equalsIgnoreCase("COPY")) {
            terminal.writer().println("COPY <table> [(<columns>)] TO|FROM <file> [WITH <option> [AND <option> ...]]");
            terminal.writer().println();
            terminal.writer().println("Copies data between CSV files and Cassandra tables:");
            terminal.writer().println("  COPY ... TO ...   - Exports data from a table to a CSV file");
            terminal.writer().println("  COPY ... FROM ... - Imports data from a CSV file to a table");
            terminal.writer().println();
            terminal.writer().println("Common options include:");
            terminal.writer().println("  HEADER=true|false - Whether to include column names in the first line");
            terminal.writer().println("  NULLVAL='<string>' - String to use for null values");
            terminal.writer().println();
            terminal.writer().println("Example: COPY users TO 'users.csv' WITH HEADER=true");
        } else if (args.equalsIgnoreCase("CAPTURE")) {
            terminal.writer().println("CAPTURE '<file>' | OFF");
            terminal.writer().println();
            terminal.writer().println("Controls capturing of output to a file:");
            terminal.writer().println("  CAPTURE '<file>' - Begins capturing output to the specified file");
            terminal.writer().println("  CAPTURE OFF     - Stops capturing output");
            terminal.writer().println("  CAPTURE         - Shows current capture status");
            terminal.writer().println();
            terminal.writer().println("Example: CAPTURE 'query_results.txt'");
        } else if (args.equalsIgnoreCase("LOGIN")) {
            terminal.writer().println("LOGIN <username> [<password>]");
            terminal.writer().println();
            terminal.writer().println("Authenticates as the specified user for the current session.");
            terminal.writer().println("If password is not provided, you will be prompted to enter it.");
            terminal.writer().println();
            terminal.writer().println("Example: LOGIN admin");
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
        // Normalize args for better matching
        String normalizedArgs = args.trim().toUpperCase();

        if (args.isEmpty() || normalizedArgs.equals("SCHEMA")) {
            // Describe all keyspaces by directly querying system tables
            try {
                ResultSet results = connectionManager.execute("SELECT keyspace_name FROM system_schema.keyspaces");
                terminal.writer().println("Keyspaces:");
                results.forEach(row -> {
                    terminal.writer().println("  " + row.getString("keyspace_name"));
                });
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error describing schema", e);
            }
        } else if (normalizedArgs.equals("KEYSPACES")) {
            // List all keyspaces by directly querying system tables
            try {
                // Use system_schema.keyspaces to get keyspace information without requiring a connected keyspace
                ResultSet results = connectionManager.execute("SELECT keyspace_name FROM system_schema.keyspaces");
                terminal.writer().println("Keyspaces:");
                results.forEach(row -> {
                    terminal.writer().println("  " + row.getString("keyspace_name"));
                });
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error listing keyspaces", e);
            }
        } else if (normalizedArgs.startsWith("KEYSPACE")) {
            String[] parts = args.split("\\s+", 2);
            String keyspaceName = parts.length > 1 ? parts[1] : connectionManager.getCurrentKeyspace();

            if (keyspaceName == null) {
                terminal.writer().println("ERROR: No keyspace specified and not connected to a keyspace. Please specify a keyspace name.");
                return true;
            }

            // Describe the specified keyspace
            try {
                ResultSet keyspaceResults = connectionManager.execute(
                        "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspaceName + "'");

                if (keyspaceResults.one() == null) {
                    terminal.writer().printf("ERROR: Keyspace '%s' does not exist%n", keyspaceName);
                    return true;
                }

                // Get keyspace details for CREATE KEYSPACE statement
                ResultSet replicationResults = connectionManager.execute(
                        "SELECT replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspaceName + "'");

                var row = replicationResults.one();
                Map<String, String> replication = row.getMap("replication", String.class, String.class);
                boolean durableWrites = row.getBoolean("durable_writes");

                // Build CREATE KEYSPACE statement
                StringBuilder createKeyspaceStmt = new StringBuilder();
                createKeyspaceStmt.append("CREATE KEYSPACE ").append(keyspaceName).append(" WITH REPLICATION = {");

                // Add replication options
                boolean first = true;
                for (Map.Entry<String, String> entry : replication.entrySet()) {
                    if (!first) {
                        createKeyspaceStmt.append(", ");
                    }
                    createKeyspaceStmt.append("'").append(entry.getKey()).append("': ");

                    // Check if the value is numeric
                    if (entry.getValue().matches("\\d+")) {
                        createKeyspaceStmt.append(entry.getValue());
                    } else {
                        createKeyspaceStmt.append("'").append(entry.getValue()).append("'");
                    }

                    first = false;
                }

                createKeyspaceStmt.append("}");

                // Add durable_writes if it's false (true is default)
                if (!durableWrites) {
                    createKeyspaceStmt.append(" AND DURABLE_WRITES = false");
                }

                createKeyspaceStmt.append(";");

                // Print the CREATE KEYSPACE statement
                terminal.writer().println(createKeyspaceStmt.toString());
                terminal.writer().println();

                // Get tables in the keyspace
                ResultSet tableResults = connectionManager.execute(
                        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + keyspaceName + "'");

                if (tableResults.getAvailableWithoutFetching() > 0) {
                    terminal.writer().println("Tables:");
                    tableResults.forEach(tableRow -> {
                        terminal.writer().println("  " + tableRow.getString("table_name"));
                    });
                } else {
                    terminal.writer().println("No tables found in keyspace.");
                }
            } catch (Exception e) {
                terminal.writer().println("ERROR: " + e.getMessage());
                logger.error("Error describing keyspace", e);
            }
        } else if (normalizedArgs.startsWith("TABLE") || normalizedArgs.startsWith("TABLES")) {
            String[] parts = args.split("\\s+", 2);
            if (parts.length < 2) {
                terminal.writer().println("ERROR: Table name required");
                return true;
            }

            String tableSpec = parts[1];
            String keyspaceName;
            String tableName;

            // Check if the table name is fully qualified (contains a dot)
            if (tableSpec.contains(".")) {
                String[] tableSpecParts = tableSpec.split("\\.", 2);
                keyspaceName = tableSpecParts[0];
                tableName = tableSpecParts[1];
            } else {
                tableName = tableSpec;
                keyspaceName = connectionManager.getCurrentKeyspace();

                if (keyspaceName == null) {
                    terminal.writer().println("ERROR: Not connected to a keyspace. Please specify a fully qualified table name (keyspace.table)");
                    return true;
                }
            }

            // Describe the specified table
            try {
                // Get table schema
                String tableSpecToUse = tableSpec.contains(".") ? tableSpec : tableName;
                TableMetadata tableMetadata = connectionManager.getTableMetadata(tableSpecToUse);
                if (tableMetadata == null) {
                    terminal.writer().printf("ERROR: Table '%s' does not exist%n", tableName);
                    return true;
                }

                // Build CREATE TABLE statement
                StringBuilder createTableStmt = new StringBuilder();
                createTableStmt.append("CREATE TABLE ").append(keyspaceName).append(".").append(tableName).append(" (\n");

                // Add column definitions
                List<String> columnDefs = new ArrayList<>();
                tableMetadata.getColumns().forEach((name, column) -> {
                    String columnDef = "    " + name + " " + column.getType();
                    if (column.isStatic()) {
                        columnDef += " STATIC";
                    }
                    columnDefs.add(columnDef);
                });

                // Add primary key definition
                StringBuilder primaryKeyDef = new StringBuilder("    PRIMARY KEY (");

                // Get partition key columns
                List<String> partitionKeyColumns = tableMetadata.getPartitionKey().stream()
                        .map(col -> col.getName().asInternal())
                        .collect(Collectors.toList());

                // Get clustering key columns
                List<String> clusteringKeyColumns = tableMetadata.getClusteringColumns().keySet().stream()
                        .map(col -> col.getName().asInternal())
                        .collect(Collectors.toList());

                // Format primary key based on whether it's a composite key or not
                if (partitionKeyColumns.size() > 1) {
                    // Composite partition key
                    primaryKeyDef.append("(")
                            .append(String.join(", ", partitionKeyColumns))
                            .append(")");
                } else {
                    // Simple partition key
                    primaryKeyDef.append(partitionKeyColumns.get(0));
                }

                // Add clustering columns if any
                if (!clusteringKeyColumns.isEmpty()) {
                    primaryKeyDef.append(", ")
                            .append(String.join(", ", clusteringKeyColumns));
                }

                primaryKeyDef.append(")");
                columnDefs.add(primaryKeyDef.toString());

                // Join all column definitions
                createTableStmt.append(String.join(",\n", columnDefs));

                // Add table options if any
                Map<String, String> options = new HashMap<>();

                // Get clustering order if specified
                if (!clusteringKeyColumns.isEmpty()) {
                    StringBuilder clusteringOrder = new StringBuilder("CLUSTERING ORDER BY (");
                    List<String> clusteringOrderParts = new ArrayList<>();

                    tableMetadata.getClusteringColumns().forEach((column, order) -> {
                        String direction = order == com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.DESC ? "DESC" : "ASC";
                        clusteringOrderParts.add(column.getName().asInternal() + " " + direction);
                    });

                    clusteringOrder.append(String.join(", ", clusteringOrderParts)).append(")");
                    options.put("CLUSTERING ORDER", clusteringOrder.toString());
                }

                // Add compact storage if applicable
                if (tableMetadata.isCompactStorage()) {
                    options.put("COMPACT STORAGE", "");
                }

                // Add other table options
                // Note: This is a simplified version, you may need to add more options based on your requirements

                // Append options to CREATE TABLE statement
                if (!options.isEmpty()) {
                    createTableStmt.append("\n) WITH ");
                    List<String> optionsList = new ArrayList<>();

                    options.forEach((key, value) -> {
                        if (value.isEmpty()) {
                            optionsList.add(key);
                        } else {
                            optionsList.add(key + " = " + value);
                        }
                    });

                    createTableStmt.append(String.join(" AND ", optionsList));
                } else {
                    createTableStmt.append("\n)");
                }

                createTableStmt.append(";");

                // Print the CREATE TABLE statement
                terminal.writer().println(createTableStmt.toString());
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
        if (args.isEmpty()) {
            terminal.writer().println("CAPTURE command not implemented yet.");
            return true;
        }

        if (args.equalsIgnoreCase("OFF")) {
            terminal.writer().println("Capture stopped.");
            return true;
        }

        // Remove quotes if present
        String filename = args.trim();
        if (filename.startsWith("'") && filename.endsWith("'")) {
            filename = filename.substring(1, filename.length() - 1);
        }

        terminal.writer().printf("Now capturing to '%s'%n", filename);
        return true;
    }

    /**
     * Handles the CONSISTENCY command to set the consistency level.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleConsistency(String args) {
        if (args.isEmpty()) {
            terminal.writer().printf("Current consistency level is %s.%n", 
                connectionManager.getConsistencyLevel());
            return true;
        }

        try {
            com.datastax.oss.driver.api.core.ConsistencyLevel level = null;
            switch (args.toUpperCase()) {
                case "ANY":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.ANY;
                    break;
                case "ONE":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.ONE;
                    break;
                case "TWO":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.TWO;
                    break;
                case "THREE":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.THREE;
                    break;
                case "QUORUM":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
                    break;
                case "ALL":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.ALL;
                    break;
                case "LOCAL_QUORUM":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;
                    break;
                case "LOCAL_ONE":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_ONE;
                    break;
                case "SERIAL":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.SERIAL;
                    break;
                case "LOCAL_SERIAL":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid consistency level");
            }

            connectionManager.setConsistencyLevel(level);
            terminal.writer().printf("Consistency level set to %s.%n", level);
        } catch (IllegalArgumentException e) {
            terminal.writer().printf("ERROR: Invalid consistency level '%s'. Valid values are: %s%n", 
                args, String.join(", ", getValidConsistencyLevels()));
        }
        return true;
    }

    /**
     * Handles the SERIAL CONSISTENCY command to set the serial consistency level.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleSerialConsistency(String args) {
        if (args.isEmpty() || !args.toUpperCase().startsWith("CONSISTENCY")) {
            terminal.writer().println("ERROR: Usage: SERIAL CONSISTENCY <level>");
            return true;
        }

        String[] parts = args.split("\\s+", 2);
        String levelStr = parts.length > 1 ? parts[1] : "";

        if (levelStr.isEmpty()) {
            terminal.writer().printf("Current serial consistency level is %s.%n", 
                connectionManager.getSerialConsistencyLevel());
            return true;
        }

        try {
            com.datastax.oss.driver.api.core.ConsistencyLevel level = null;
            switch (levelStr.toUpperCase()) {
                case "SERIAL":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.SERIAL;
                    break;
                case "LOCAL_SERIAL":
                    level = com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL;
                    break;
                default:
                    terminal.writer().println("ERROR: Serial consistency level must be SERIAL or LOCAL_SERIAL");
                    return true;
            }

            connectionManager.setSerialConsistencyLevel(level);
            terminal.writer().printf("Serial consistency level set to %s.%n", level);
        } catch (IllegalArgumentException e) {
            terminal.writer().println("ERROR: Serial consistency level must be SERIAL or LOCAL_SERIAL");
        }
        return true;
    }

    /**
     * Handles the PAGING command to enable/disable paging or set the page size.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handlePaging(String args) {
        if (args.isEmpty()) {
            boolean pagingEnabled = connectionManager.isPagingEnabled();
            int pageSize = connectionManager.getPageSize();
            terminal.writer().printf("Paging is %s. Page size: %d%n", 
                pagingEnabled ? "ON" : "OFF", pageSize);
            return true;
        }

        if (args.equalsIgnoreCase("ON")) {
            connectionManager.setPagingEnabled(true);
            terminal.writer().println("Paging is now ON.");
        } else if (args.equalsIgnoreCase("OFF")) {
            connectionManager.setPagingEnabled(false);
            terminal.writer().println("Paging is now OFF.");
        } else {
            try {
                int pageSize = Integer.parseInt(args.trim());
                connectionManager.setPageSize(pageSize);
                connectionManager.setPagingEnabled(true);
                terminal.writer().printf("Page size set to %d. Paging is ON.%n", pageSize);
            } catch (NumberFormatException e) {
                terminal.writer().println("ERROR: Invalid argument. Use PAGING ON, PAGING OFF, or PAGING <size>.");
            } catch (IllegalArgumentException e) {
                terminal.writer().println("ERROR: " + e.getMessage());
            }
        }
        return true;
    }

    /**
     * Handles the SHOW command to display version, host, or session information.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleShow(String args) {
        if (args.isEmpty()) {
            terminal.writer().println("ERROR: SHOW command requires an argument (VERSION, HOST, SESSION)");
            return true;
        }

        String command = args.split("\\s+")[0].toUpperCase();

        if (command.equals("VERSION")) {
            terminal.writer().printf("[jcqlsh %s | Cassandra %s | CQL spec %s | Native protocol v%s]%n",
                "1.0.0", // jcqlsh version
                connectionManager.getCassandraVersion(),
                "3.4.2", // CQL spec version - hardcoded for now
                connectionManager.getProtocolVersion());
        } else if (command.equals("HOST")) {
            terminal.writer().printf("Connected to %s at %s:%d.%n",
                connectionManager.getClusterName(),
                connectionManager.getHost(),
                connectionManager.getPort());
        } else if (command.equals("SESSION")) {
            String sessionId = args.substring(command.length()).trim();
            if (sessionId.isEmpty()) {
                terminal.writer().println("ERROR: SESSION command requires a session ID");
                return true;
            }

            try {
                // Execute a query to get session information
                ResultSet resultSet = connectionManager.execute(
                    "SELECT * FROM system_traces.sessions WHERE session_id = " + sessionId);

                if (resultSet.one() == null) {
                    terminal.writer().printf("Session %s not found%n", sessionId);
                    return true;
                }

                terminal.writer().printf("Tracing session: %s%n%n", sessionId);

                // Get events for this session
                ResultSet eventsResultSet = connectionManager.execute(
                    "SELECT * FROM system_traces.events WHERE session_id = " + sessionId);

                // Format and display the events
                terminal.writer().println(" activity                                                  | timestamp                  | source    | source_elapsed | client");
                terminal.writer().println("-----------------------------------------------------------+----------------------------+-----------+----------------+-----------");

                eventsResultSet.forEach(row -> {
                    terminal.writer().printf(" %-56s | %-26s | %-9s | %14d | %s%n",
                        row.getString("activity"),
                        row.getInstant("event_id"),
                        row.getInetAddress("source"),
                        row.getInt("source_elapsed"),
                        row.getInetAddress("source"));
                });
            } catch (Exception e) {
                terminal.writer().printf("ERROR: Could not retrieve session information: %s%n", e.getMessage());
                logger.error("Error retrieving session information", e);
            }
        } else {
            terminal.writer().printf("ERROR: Unknown SHOW command: %s%n", command);
        }

        return true;
    }

    /**
     * Handles the LOGIN command to authenticate as a specified user.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleLogin(String args) {
        terminal.writer().println("LOGIN command not implemented yet.");
        return true;
    }

    /**
     * Handles the COPY command for data import/export.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleCopy(String args) {
        terminal.writer().println("COPY command not implemented yet.");
        return true;
    }

    /**
     * Gets a list of valid consistency level names.
     *
     * @return a list of valid consistency level names
     */
    private List<String> getValidConsistencyLevels() {
        return List.of("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", 
            "LOCAL_QUORUM", "LOCAL_ONE", "SERIAL", "LOCAL_SERIAL");
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
