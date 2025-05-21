package com.github.jshook.commands;

import com.github.jshook.connection.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command that enables or disables query tracing.
 */
public class TracingCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(TracingCommand.class);

    private static final String HELP_MESSAGE = 
            "TRACING - Controls query tracing\n" +
            "Usage: TRACING (ON | OFF | STATUS)\n" +
            "  ON - Enables query tracing\n" +
            "  OFF - Disables query tracing\n" +
            "  STATUS - Shows the current tracing status";

    private final ConnectionManager connectionManager;

    public TracingCommand(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }

        String option = args.trim().toUpperCase();
        
        if (option.isEmpty() || option.equals("STATUS")) {
            boolean enabled = connectionManager.isTracingEnabled();
            System.out.println("Tracing is currently " + (enabled ? "enabled" : "disabled"));
        } else if (option.equals("ON")) {
            connectionManager.setTracingEnabled(true);
            System.out.println("Tracing enabled");
        } else if (option.equals("OFF")) {
            connectionManager.setTracingEnabled(false);
            System.out.println("Tracing disabled");
        } else {
            System.out.println("Unknown tracing option: " + option);
            System.out.println(HELP_MESSAGE);
        }

        return true;
    }

    @Override
    public String getName() {
        return "TRACING";
    }

    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}