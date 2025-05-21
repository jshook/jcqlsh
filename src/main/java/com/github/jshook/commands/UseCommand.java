package com.github.jshook.commands;

import com.github.jshook.connection.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command that changes the current keyspace.
 */
public class UseCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(UseCommand.class);

    private static final String HELP_MESSAGE = 
            "USE - Changes the current keyspace\n" +
            "Usage: USE <keyspace>\n" +
            "Changes the current keyspace to the specified keyspace.";

    private final ConnectionManager connectionManager;

    public UseCommand(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }

        String keyspaceName = args.trim();
        
        if (keyspaceName.isEmpty()) {
            System.out.println("Error: Keyspace name is required");
            System.out.println(HELP_MESSAGE);
            return true;
        }

        try {
            connectionManager.useKeyspace(keyspaceName);
            System.out.println("Now using keyspace " + keyspaceName);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            logger.error("Error changing keyspace", e);
        }

        return true;
    }

    @Override
    public String getName() {
        return "USE";
    }

    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}