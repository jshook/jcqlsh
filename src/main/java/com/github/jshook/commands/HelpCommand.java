package com.github.jshook.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jshook.shell.CommandRegistry;

/**
 * Command that displays help information about available commands.
 */
public class HelpCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(HelpCommand.class);

    private static final String HELP_MESSAGE = 
            "HELP - Displays help information about available commands\n" +
            "Usage: HELP [COMMAND]\n" +
            "If COMMAND is specified, displays detailed help for that command.\n" +
            "Otherwise, displays a list of all available commands.";

    private final CommandRegistry commandRegistry;

    public HelpCommand(CommandRegistry commandRegistry) {
        this.commandRegistry = commandRegistry;
    }

    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }

        String commandName = args.trim().toUpperCase();

        if (commandName.isEmpty()) {
            // List all registered commands
            System.out.println("Available commands:");
            for (String name : commandRegistry.getCommandNames()) {
                System.out.println("  " + name);
            }
            System.out.println();
            System.out.println("Type HELP <command> for more information on a specific command.");
        } else {
            Command cmd = commandRegistry.getCommand(commandName);
            if (cmd != null) {
                System.out.println(cmd.getHelp());
            } else {
                System.out.println("No help available for command: " + commandName);
            }
        }

        return true;
    }

    @Override
    public String getName() {
        return "HELP";
    }

    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}
