package com.github.jshook.shell;

import java.util.HashMap;
import java.util.Map;

import com.github.jshook.commands.Command;
import com.github.jshook.commands.DescribeCommand;
import com.github.jshook.commands.ExitCommand;
import com.github.jshook.commands.HelpCommand;
import com.github.jshook.commands.ShowCommand;
import com.github.jshook.commands.TracingCommand;
import com.github.jshook.commands.UseCommand;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.config.FormattingConfig;

/**
 * Registry for shell commands, mapping command names to implementations.
 */
public class CommandRegistry {
    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;
    private final Map<String, Command> commands = new HashMap<>();

    /**
     * Creates a new CommandRegistry with default commands registered.
     *
     * @param connectionManager  the connection manager for DB-related commands
     * @param formattingConfig   the formatting configuration for output commands
     */
    public CommandRegistry(ConnectionManager connectionManager, FormattingConfig formattingConfig) {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
        registerDefaultCommands();
    }

    /**
     * Registers a command implementation.
     *
     * @param command the command to register
     */
    public void registerCommand(Command command) {
        commands.put(command.getName().toUpperCase(), command);
    }
    /**
     * Registers a simple handler function as a command.
     *
     * @param name    the command name
     * @param handler the function to execute for this command
     */
    public void register(String name, java.util.function.Function<String, Boolean> handler) {
        commands.put(name.toUpperCase(), new Command() {
            @Override
            public boolean execute(String args) {
                return handler.apply(args);
            }
            @Override
            public String getName() {
                return name.toUpperCase();
            }
            @Override
            public String getHelp() {
                return "";
            }
        });
    }

    /**
     * Checks if a command name is registered.
     *
     * @param name the command name
     * @return true if registered, false otherwise
     */
    public boolean isRegistered(String name) {
        return commands.containsKey(name.toUpperCase());
    }

    /**
     * Executes a registered command.
     *
     * @param name the command name
     * @param args the command arguments
     * @return true to continue shell loop, false to exit or break
     */
    public boolean execute(String name, String args) {
        Command cmd = commands.get(name.toUpperCase());
        if (cmd != null) {
            return cmd.execute(args);
        }
        return false;
    }
    /**
     * Gets the command implementation for the given name.
     *
     * @param name the command name
     * @return the Command, or null if not found
     */
    public Command getCommand(String name) {
        return commands.get(name.toUpperCase());
    }
    /**
     * Gets the set of registered command names.
     *
     * @return a set of command names
     */
    public java.util.Set<String> getCommandNames() {
        return java.util.Collections.unmodifiableSet(commands.keySet());
    }

    /**
     * Registers the default set of commands.
     */
    public void registerDefaultCommands() {
        registerCommand(new HelpCommand(this));
        registerCommand(new ExitCommand());
        registerCommand(new UseCommand(connectionManager));
        registerCommand(new DescribeCommand(connectionManager, formattingConfig));
        registerCommand(new TracingCommand(connectionManager));
        registerCommand(new ShowCommand(connectionManager, formattingConfig));
    }
}
