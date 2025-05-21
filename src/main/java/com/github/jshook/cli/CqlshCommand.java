package com.github.jshook.cli;

import com.github.jshook.config.ConnectionConfig;
import com.github.jshook.config.FormattingConfig;
import com.github.jshook.config.OutputFormat;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.script.ScriptExecutor;
import com.github.jshook.shell.InteractiveShell;
import com.github.jshook.shell.LanternaShell;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Main command class for the CQL shell, handling CLI arguments and launching
 * either interactive mode or executing script files.
 */
@Command(
    name = "jcqlsh",
    description = "Java Cassandra Query Language Shell",
    mixinStandardHelpOptions = true,
    version = "1.0"
)
public class CqlshCommand implements Callable<Integer> {

    @Spec
    private CommandSpec spec;

    // Connection parameters
    @Option(names = {"-h", "--host"}, description = "Cassandra host address")
    private String host = "localhost";

    @Option(names = {"-p", "--port"}, description = "Cassandra port number")
    private int port = 9042;

    @Option(names = {"-u", "--username"}, description = "Username for authentication")
    private String username;

    @Option(names = {"--password"}, description = "Password for authentication", interactive = true, arity = "0..1")
    private String password;

    @Option(names = {"-k", "--keyspace"}, description = "Keyspace to use")
    private String keyspace;

    @Option(names = {"--dc"}, description = "Local data center name")
    private String localDatacenter;

    // SSL/TLS Options
    @Option(names = {"--ssl"}, description = "Use SSL connection")
    private boolean useSsl = false;

    @Option(names = {"--ssl-truststore"}, description = "Path to SSL truststore")
    private File sslTruststore;

    @Option(names = {"--ssl-truststore-password"}, description = "SSL truststore password", interactive = true)
    private String sslTruststorePassword;

    // Timeout settings
    @Option(names = {"--connect-timeout"}, description = "Connection timeout in seconds")
    private int connectTimeout = 5;

    @Option(names = {"--request-timeout"}, description = "Request timeout in seconds")
    private int requestTimeout = 10;

    // Output formatting
    @Option(names = {"--output-format"}, description = "Output format (tabular, json, csv)")
    private OutputFormat outputFormat = OutputFormat.TABULAR;

    @Option(names = {"--no-color"}, description = "Disable colored output")
    private boolean noColor = false;

    @Option(names = {"--max-width"}, description = "Maximum width for tabular output")
    private int maxWidth = 100;

    // UI options
    @Option(names = {"--ui"}, description = "UI mode (terminal, lanterna)")
    private String uiMode = "terminal";

    // Script execution
    @Option(names = {"-f", "--file"}, description = "Execute commands from file")
    private File file;

    @Option(names = {"--debug"}, description = "Enable debug mode")
    private boolean debug = false;

    @Parameters(description = "CQL statement to execute")
    private String[] statements;

    // Field to track if we're in interactive mode
    private boolean interactiveMode = false;

    @Override
    public Integer call() throws Exception {
        // Logging is configured via logback.xml with default level WARNING
        // If debug flag is enabled, programmatically set root logger to DEBUG level
        if (debug) {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) 
                org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            root.setLevel(ch.qos.logback.classic.Level.DEBUG);
        }

        // Create connection config from parameters
        ConnectionConfig connectionConfig = new ConnectionConfig(
            host, 
            port, 
            username, 
            password, 
            keyspace, 
            Duration.ofSeconds(connectTimeout),
            Duration.ofSeconds(requestTimeout),
            useSsl,
            sslTruststore != null ? sslTruststore.getPath() : null,
            sslTruststorePassword,
            localDatacenter
        );

        // Create formatting config
        FormattingConfig formattingConfig = new FormattingConfig(
            outputFormat,
            false, // expanded view
            maxWidth,
            !noColor
        );

        // Initialize connection manager
        ConnectionManager connectionManager = new ConnectionManager(connectionConfig);

        try {
            // Connect to Cassandra
            connectionManager.connect();

            // If a file is specified, execute it
            if (file != null) {
                ScriptExecutor executor = new ScriptExecutor(connectionManager, formattingConfig);
                return executor.executeFile(file) ? 0 : 1;
            }

            // If statements are provided as arguments, execute them
            if (statements != null && statements.length > 0) {
                String cql = String.join(" ", statements);
                ScriptExecutor executor = new ScriptExecutor(connectionManager, formattingConfig);
                return executor.executeStatement(cql) ? 0 : 1;
            }

            // Otherwise, start interactive mode
            interactiveMode = true;

            // Choose the appropriate shell based on the UI mode
                if ("lanterna".equalsIgnoreCase(uiMode)) {
                // Use the lanterna-based UI
                LanternaShell
                    shell = new LanternaShell(connectionManager, formattingConfig);
                return shell.start();
            } else {
                // Use the traditional terminal UI
                InteractiveShell shell = new InteractiveShell(connectionManager, formattingConfig);
                return shell.start();
            }
        } catch (Exception e) {
            // Only print stack trace in debug mode, otherwise just print the error message
            if (debug) {
                System.err.println("Error (debug mode): ");
                e.printStackTrace();
            } else {
                System.err.println("Error: " + e.getMessage());
            }
            return 1;
        }
    }

    public boolean isInteractiveMode() {
        return interactiveMode;
    }
}
