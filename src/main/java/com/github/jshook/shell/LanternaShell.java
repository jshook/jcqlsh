package com.github.jshook.shell;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.github.jshook.output.ResultFormatter;
import com.github.jshook.output.ResultFormatterFactory;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.Borders;
import com.googlecode.lanterna.gui2.DefaultWindowManager;
import com.googlecode.lanterna.gui2.Direction;
import com.googlecode.lanterna.gui2.EmptySpace;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.WindowBasedTextGUI;
import com.googlecode.lanterna.input.KeyType;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lanterna-based text UI implementation for the CQL shell.
 */
public class LanternaShell {
    private static final Logger logger = LoggerFactory.getLogger(LanternaShell.class);

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

    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;
    private final ResultFormatterFactory formatterFactory;
    private final CommandRegistry commandRegistry;

    private Terminal terminal;
    private Screen screen;
    private WindowBasedTextGUI textGUI;
    private TextBox outputBox;
    private TextBox inputBox;
    private Label statusLabel;
    private boolean running = true;

    /**
     * Creates a new LanternaShell with the given connection manager and formatting configuration.
     *
     * @param connectionManager the connection manager to use for executing queries
     * @param formattingConfig  the formatting configuration to use for displaying results
     */
    public LanternaShell(ConnectionManager connectionManager, FormattingConfig formattingConfig) {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
        this.formatterFactory = new ResultFormatterFactory(formattingConfig);
        this.commandRegistry = new CommandRegistry();

        // Register special commands
        registerSpecialCommands();
    }

    /**
     * Starts the interactive shell, displaying a welcome message and entering the command loop.
     *
     * @return 0 if the shell exits normally, non-zero otherwise
     */
    public int start() {
        try {
            initializeUI();
            setupLayout();
            displayWelcomeMessage();

            // Start the GUI
            textGUI.addWindowAndWait(createMainWindow());

            return 0;
        } catch (Exception e) {
            logger.error("Error starting Lanterna shell", e);
            return 1;
        } finally {
            try {
                if (screen != null) {
                    screen.stopScreen();
                }
                if (terminal != null) {
                    terminal.close();
                }
            } catch (IOException e) {
                logger.error("Error closing terminal", e);
            }
        }
    }

    /**
     * Initializes the Lanterna UI components.
     *
     * @throws IOException if there is an error initializing the terminal
     */
    private void initializeUI() throws IOException {
        // Create terminal and screen
        terminal = new DefaultTerminalFactory().createTerminal();
        screen = new TerminalScreen(terminal);
        screen.startScreen();

        // Create GUI and window manager
        textGUI = new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(TextColor.ANSI.BLACK));
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
     * Performs autocompletion on the current input.
     *
     * @param input the current input text
     * @return the autocompleted text, or the original input if no completion is available
     */
    private String autocomplete(String input) {
        if (input == null || input.trim().isEmpty()) {
            return input;
        }

        String trimmedInput = input.trim();
        String lowerInput = trimmedInput.toLowerCase();

        // Get the last word in the input (the one we're trying to complete)
        int lastSpaceIndex = trimmedInput.lastIndexOf(' ');
        String lastWord = lastSpaceIndex == -1 ? trimmedInput : trimmedInput.substring(lastSpaceIndex + 1);
        String prefix = lastSpaceIndex == -1 ? "" : trimmedInput.substring(0, lastSpaceIndex + 1);

        // If the input is empty or we're at the start of a new word, suggest keywords and special commands
        if (lastWord.isEmpty()) {
            // Default to the first keyword if available
            if (CQL_KEYWORDS.length > 0) {
                return prefix + CQL_KEYWORDS[0];
            }
            return input;
        }

        // If we're after a USE keyword, suggest keyspaces
        if (isAfterKeyword(lowerInput, "use")) {
            try {
                List<String> keyspaces = connectionManager.getKeyspaces();
                for (String keyspace : keyspaces) {
                    if (keyspace.toLowerCase().startsWith(lastWord.toLowerCase())) {
                        return prefix + keyspace;
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving keyspaces for completion", e);
            }
            return input;
        }

        // If we're after a FROM, UPDATE, INTO, or JOIN keyword, suggest tables
        if (isAfterKeyword(lowerInput, "from") || isAfterKeyword(lowerInput, "update") || 
            isAfterKeyword(lowerInput, "into") || isAfterKeyword(lowerInput, "join")) {
            try {
                String currentKeyspace = connectionManager.getCurrentKeyspace();
                if (currentKeyspace != null) {
                    List<String> tables = connectionManager.getTables(currentKeyspace);
                    for (String table : tables) {
                        if (table.toLowerCase().startsWith(lastWord.toLowerCase())) {
                            return prefix + table;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving tables for completion", e);
            }
            return input;
        }

        // If we're after a SELECT keyword or in a WHERE clause, suggest columns
        if (isAfterKeyword(lowerInput, "select") || isAfterKeyword(lowerInput, "where") || 
            isAfterKeyword(lowerInput, "set") || lowerInput.contains("=")) {
            try {
                String tableName = extractTableName(lowerInput);
                if (tableName != null) {
                    List<String> columns = getColumnNames(tableName);
                    for (String column : columns) {
                        if (column.toLowerCase().startsWith(lastWord.toLowerCase())) {
                            return prefix + column;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving columns for completion", e);
            }
            return input;
        }

        // Default to suggesting keywords
        for (String keyword : CQL_KEYWORDS) {
            if (keyword.toLowerCase().startsWith(lastWord.toLowerCase())) {
                return prefix + keyword;
            }
        }

        // If no completion is available, return the original input
        return input;
    }

    /**
     * Creates the main window for the shell.
     *
     * @return the main window
     */
    private Window createMainWindow() {
        // Create a basic window
        BasicWindow window = new BasicWindow("JCqlsh - Java Cassandra Query Language Shell");
        window.setHints(List.of(Window.Hint.FULL_SCREEN));

        Panel mainPanel = new Panel(new LinearLayout(Direction.VERTICAL));

        // Output area
        outputBox = new TextBox(new TerminalSize(80, 20));
        outputBox.setReadOnly(true);
        outputBox.setVerticalFocusSwitching(false);

        // Status bar
        statusLabel = new Label("");
        updateStatusBar();

        // Input area
        inputBox = new TextBox(new TerminalSize(80, 1));

        // Add components to the panel
        mainPanel.addComponent(new Label("Output:"));
        mainPanel.addComponent(outputBox.withBorder(Borders.singleLine()));
        mainPanel.addComponent(statusLabel);
        mainPanel.addComponent(new Label("Enter command:"));
        mainPanel.addComponent(inputBox);

        // Add help text
        mainPanel.addComponent(new Label("Press Tab for autocompletion, Ctrl+C to exit, Enter to execute command"));

        window.setComponent(mainPanel);

        // Handle window close
        window.setCloseWindowWithEscape(true);

        // Add a custom key handler to handle Ctrl+C and Tab
        inputBox.setInputFilter((interactionEvent, keyStroke) -> {
            // Handle Enter key to execute commands
            if (keyStroke.getCharacter() == '\n') {
                String command = inputBox.getText();
                inputBox.setText("");
                processCommand(command);
                return false; // Don't add the newline to the input
            }

            // Handle Tab key for autocompletion
            if (keyStroke.getKeyType() == KeyType.Tab) {
                String currentText = inputBox.getText();
                String completedText = autocomplete(currentText);
                if (!completedText.equals(currentText)) {
                    inputBox.setText(completedText);
                    inputBox.setCaretPosition(completedText.length());
                }
                return false; // Don't add the tab character to the input
            }

            // Handle Ctrl+C to exit
            if (keyStroke.isCtrlDown() && keyStroke.getCharacter() == 'c') {
                window.close();
                return false; // Don't add the character to the input
            }

            return true; // Accept all other keys
        });

        return window;
    }

    /**
     * Sets up the layout for the UI.
     */
    private void setupLayout() {
        // No additional layout setup needed for now
    }

    /**
     * Displays a welcome message in the output area.
     */
    private void displayWelcomeMessage() {
        StringBuilder welcome = new StringBuilder();
        welcome.append("Java CQL Shell (JCqlsh) v1.0.0\n");
        welcome.append("Type 'help' for help, 'quit' to exit.\n\n");

        // Display connection information
        try {
            String clusterName = connectionManager.execute("SELECT cluster_name FROM system.local").one().getString("cluster_name");
            welcome.append(String.format("Connected to %s at %s:%d\n\n", 
                    clusterName, 
                    connectionManager.getHost(), 
                    connectionManager.getPort()));
        } catch (Exception e) {
            logger.warn("Could not get cluster name", e);
            welcome.append("Connected to Cassandra\n\n");
        }

        appendToOutput(welcome.toString());
    }

    /**
     * Updates the status bar with current connection information.
     */
    private void updateStatusBar() {
        String currentKeyspace = connectionManager.getCurrentKeyspace();
        String keyspaceInfo = (currentKeyspace == null) ? "No keyspace selected" : "Keyspace: " + currentKeyspace;
        statusLabel.setText(keyspaceInfo + " | " + connectionManager.getHost() + ":" + connectionManager.getPort());
    }

    /**
     * Processes a command entered by the user.
     *
     * @param command the command to process
     */
    private void processCommand(String command) {
        if (command == null || command.trim().isEmpty()) {
            return;
        }

        command = command.trim();
        appendToOutput("> " + command + "\n");

        try {
            if (isSpecialCommand(command)) {
                boolean shouldContinue = handleSpecialCommand(command);
                if (!shouldContinue) {
                    textGUI.getActiveWindow().close();
                }
            } else {
                executeCql(command);
            }
        } catch (Exception e) {
            appendToOutput("ERROR: " + e.getMessage() + "\n");
            logger.error("Error processing command", e);
        }

        updateStatusBar();
    }

    /**
     * Appends text to the output area.
     *
     * @param text the text to append
     */
    private void appendToOutput(String text) {
        outputBox.setText(outputBox.getText() + text);
        // Scroll to the bottom
        outputBox.setCaretPosition(outputBox.getLineCount(), 0);
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

            appendToOutput(formattedResult + "\n");

            // Show execution time if applicable
            if (resultSet.getExecutionInfo() != null) {
                appendToOutput(String.format("(%d rows in %.3f sec)%n", 
                    resultSet.getAvailableWithoutFetching(), 
                    (endTime - startTime) / 1000.0));
            }
        } catch (QueryExecutionException e) {
            appendToOutput("ERROR: " + e.getMessage() + "\n");
            logger.error("Query execution error", e);
        }
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
        commandRegistry.register("PAGING", this::handlePaging);

        // Query settings
        commandRegistry.register("CONSISTENCY", this::handleConsistency);
        commandRegistry.register("SERIAL", this::handleSerialConsistency);

        // Information commands
        commandRegistry.register("SHOW", this::handleShow);
    }

    /**
     * Handles the HELP command.
     *
     * @param args the arguments to the command
     * @return true to continue running the shell
     */
    private boolean handleHelp(String args) {
        if (args.isEmpty()) {
            appendToOutput("Available commands:\n\n");
            appendToOutput("HELP                      - Show this help message\n");
            appendToOutput("EXIT, QUIT                - Exit the shell\n");
            appendToOutput("CLEAR                     - Clear the output area\n");
            appendToOutput("USE <keyspace>            - Switch to a keyspace\n");
            appendToOutput("DESCRIBE [object]         - Describe a keyspace, table, or other object\n");
            appendToOutput("EXPAND ON|OFF             - Toggle expanded (vertical) output\n");
            appendToOutput("TRACING ON|OFF            - Toggle query tracing\n");
            appendToOutput("PAGING ON|OFF|<size>      - Control paging of query results\n");
            appendToOutput("CONSISTENCY <level>       - Set consistency level for queries\n");
            appendToOutput("SERIAL CONSISTENCY <level>- Set serial consistency level for conditional updates\n");
            appendToOutput("SHOW VERSION              - Show version information\n");
            appendToOutput("SHOW HOST                 - Show connection information\n");
            appendToOutput("\nFor help on CQL, type 'HELP CQL'\n");
        } else if (args.equalsIgnoreCase("CQL")) {
            appendToOutput("CQL (Cassandra Query Language) Quick Reference:\n\n");
            appendToOutput("Data Definition:\n");
            appendToOutput("  CREATE KEYSPACE <name> WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': N }\n");
            appendToOutput("  CREATE TABLE <name> (<column> <type> PRIMARY KEY, <column> <type>, ...)\n");
            appendToOutput("  ALTER TABLE <name> ADD <column> <type>\n");
            appendToOutput("  DROP TABLE <name>\n");
            appendToOutput("  DROP KEYSPACE <name>\n\n");
            appendToOutput("Data Manipulation:\n");
            appendToOutput("  INSERT INTO <table> (<columns>) VALUES (<values>)\n");
            appendToOutput("  UPDATE <table> SET <assignments> WHERE <conditions>\n");
            appendToOutput("  DELETE FROM <table> WHERE <conditions>\n");
            appendToOutput("  SELECT <columns> FROM <table> WHERE <conditions>\n");
        } else {
            appendToOutput("No help available for: " + args + "\n");
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
        outputBox.setText("");
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
            appendToOutput("ERROR: Keyspace name required\n");
            return true;
        }

        try {
            connectionManager.useKeyspace(keyspaceName.trim());
            appendToOutput("Now using keyspace " + keyspaceName.trim() + "\n");
        } catch (QueryExecutionException e) {
            appendToOutput("ERROR: " + e.getMessage() + "\n");
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
        // Simplified implementation - just execute the DESCRIBE command as a CQL query
        try {
            if (args.isEmpty()) {
                // List all keyspaces
                ResultSet results = connectionManager.execute("SELECT keyspace_name FROM system_schema.keyspaces");
                appendToOutput("Keyspaces:\n");
                results.forEach(row -> {
                    appendToOutput("  " + row.getString("keyspace_name") + "\n");
                });
            } else {
                appendToOutput("DESCRIBE command with arguments not fully implemented in this UI version.\n");
                appendToOutput("Please use the standard CQL queries instead.\n");
            }
        } catch (Exception e) {
            appendToOutput("ERROR: " + e.getMessage() + "\n");
            logger.error("Error describing schema", e);
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
            appendToOutput("Expanded display is " + (expanded ? "on" : "off") + ".\n");
        } else if (args.equalsIgnoreCase("ON")) {
            formattingConfig.setExpandedFormat(true);
            appendToOutput("Expanded display is now on.\n");
        } else if (args.equalsIgnoreCase("OFF")) {
            formattingConfig.setExpandedFormat(false);
            appendToOutput("Expanded display is now off.\n");
        } else {
            appendToOutput("ERROR: Invalid argument. Use EXPAND ON or EXPAND OFF.\n");
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
            appendToOutput("Tracing is " + (tracing ? "on" : "off") + ".\n");
        } else if (args.equalsIgnoreCase("ON")) {
            connectionManager.setTracingEnabled(true);
            appendToOutput("Tracing is now on.\n");
        } else if (args.equalsIgnoreCase("OFF")) {
            connectionManager.setTracingEnabled(false);
            appendToOutput("Tracing is now off.\n");
        } else {
            appendToOutput("ERROR: Invalid argument. Use TRACING ON or TRACING OFF.\n");
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
            appendToOutput("Paging is " + (pagingEnabled ? "ON" : "OFF") + ". Page size: " + pageSize + "\n");
            return true;
        }

        if (args.equalsIgnoreCase("ON")) {
            connectionManager.setPagingEnabled(true);
            appendToOutput("Paging is now ON.\n");
        } else if (args.equalsIgnoreCase("OFF")) {
            connectionManager.setPagingEnabled(false);
            appendToOutput("Paging is now OFF.\n");
        } else {
            try {
                int pageSize = Integer.parseInt(args.trim());
                connectionManager.setPageSize(pageSize);
                connectionManager.setPagingEnabled(true);
                appendToOutput("Page size set to " + pageSize + ". Paging is ON.\n");
            } catch (NumberFormatException e) {
                appendToOutput("ERROR: Invalid argument. Use PAGING ON, PAGING OFF, or PAGING <size>.\n");
            } catch (IllegalArgumentException e) {
                appendToOutput("ERROR: " + e.getMessage() + "\n");
            }
        }
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
            appendToOutput("Current consistency level is " + connectionManager.getConsistencyLevel() + ".\n");
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
            appendToOutput("Consistency level set to " + level + ".\n");
        } catch (IllegalArgumentException e) {
            appendToOutput("ERROR: Invalid consistency level '" + args + "'. Valid values are: " + 
                String.join(", ", getValidConsistencyLevels()) + "\n");
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
            appendToOutput("ERROR: Usage: SERIAL CONSISTENCY <level>\n");
            return true;
        }

        String[] parts = args.split("\\s+", 2);
        String levelStr = parts.length > 1 ? parts[1] : "";

        if (levelStr.isEmpty()) {
            appendToOutput("Current serial consistency level is " + connectionManager.getSerialConsistencyLevel() + ".\n");
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
                    appendToOutput("ERROR: Serial consistency level must be SERIAL or LOCAL_SERIAL\n");
                    return true;
            }

            connectionManager.setSerialConsistencyLevel(level);
            appendToOutput("Serial consistency level set to " + level + ".\n");
        } catch (IllegalArgumentException e) {
            appendToOutput("ERROR: Serial consistency level must be SERIAL or LOCAL_SERIAL\n");
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
            appendToOutput("ERROR: SHOW command requires an argument (VERSION, HOST)\n");
            return true;
        }

        String command = args.split("\\s+")[0].toUpperCase();

        if (command.equals("VERSION")) {
            appendToOutput(String.format("[jcqlsh %s | Cassandra %s | CQL spec %s | Native protocol v%s]%n",
                "1.0.0", // jcqlsh version
                connectionManager.getCassandraVersion(),
                "3.4.2", // CQL spec version - hardcoded for now
                connectionManager.getProtocolVersion()));
        } else if (command.equals("HOST")) {
            appendToOutput(String.format("Connected to %s at %s:%d.%n",
                connectionManager.getClusterName(),
                connectionManager.getHost(),
                connectionManager.getPort()));
        } else {
            appendToOutput("ERROR: Unknown SHOW command: " + command + "\n");
        }

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
        private final java.util.Map<String, CommandHandler> commands = new java.util.HashMap<>();

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
