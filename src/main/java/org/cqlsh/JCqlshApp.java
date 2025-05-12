package org.cqlsh;

import picocli.CommandLine;
import org.cqlsh.cli.CqlshCommand;

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

        // Always exit with the appropriate exit code
        System.exit(exitCode);
    }
}
