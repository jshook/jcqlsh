package com.github.jshook.commands;

/**
 * Interface for commands that can be executed in the shell.
 */
public interface Command {
    /**
     * Executes the command with the given arguments.
     * @param args the arguments for the command
     * @return true if the command was executed successfully, false otherwise
     */
    boolean execute(String args);

    /**
     * Gets the name of the command.
     * @return the name of the command
     */
    String getName();

    /**
     * Gets the help message for the command.
     * @return the help message
     */
    String getHelp();
}