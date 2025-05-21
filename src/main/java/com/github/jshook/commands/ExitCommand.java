package com.github.jshook.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command that exits the shell.
 */
public class ExitCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(ExitCommand.class);

    private static final String HELP_MESSAGE = 
            "EXIT - Exits the shell\n" +
            "Usage: EXIT\n" +
            "Terminates the current session and exits the shell.";

    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }

        System.out.println("Exiting shell...");
        // Return false to indicate that the shell should exit
        return false;
    }

    @Override
    public String getName() {
        return "EXIT";
    }

    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}