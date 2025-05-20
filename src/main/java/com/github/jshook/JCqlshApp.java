package com.github.jshook;

import picocli.CommandLine;
import com.github.jshook.cli.CqlshCommand;

/**
 * Main entry point for JCqlsh - Java implementation of the Cassandra Query Language Shell.
 */
public class JCqlshApp {
    public static void main(String[] args) {
        CqlshCommand command = new CqlshCommand();
        int exitCode = new CommandLine(command)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setColorScheme(CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO))
                .execute(args);
        
        // Only exit if we're not in interactive mode (for script execution)
        if (!command.isInteractiveMode()) {
            System.exit(exitCode);
        }
    }
}