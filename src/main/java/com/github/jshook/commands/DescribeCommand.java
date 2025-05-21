package com.github.jshook.commands;

import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import com.github.jshook.formatting.TableFormatter;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command that describes database objects.
 */
public class DescribeCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(DescribeCommand.class);

    private static final String HELP_MESSAGE = 
            "DESCRIBE - Describes database objects\n" +
            "Usage: DESCRIBE [FULL] (KEYSPACES | TABLES | <table_name>)\n" +
            "  KEYSPACES - Lists all keyspaces\n" +
            "  TABLES - Lists all tables in the current keyspace\n" +
            "  <table_name> - Describes the specified table\n" +
            "  FULL - Includes additional details in the output";

    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;

    public DescribeCommand(ConnectionManager connectionManager, FormattingConfig formattingConfig) {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
    }

    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }

        String[] parts = args.trim().split("\\s+");
        boolean full = false;
        String objectType;

        if (parts.length > 0 && parts[0].equalsIgnoreCase("FULL")) {
            full = true;
            objectType = parts.length > 1 ? parts[1] : "";
        } else {
            objectType = parts.length > 0 ? parts[0] : "";
        }

        if (objectType.isEmpty()) {
            System.out.println("Error: Object type is required");
            System.out.println(HELP_MESSAGE);
            return true;
        }

        try {
            if (objectType.equalsIgnoreCase("KEYSPACES")) {
                describeKeyspaces(full);
            } else if (objectType.equalsIgnoreCase("TABLES")) {
                describeTables(full);
            } else {
                describeTable(objectType, full);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            logger.error("Error describing object", e);
        }

        return true;
    }

    private void describeKeyspaces(boolean full) {
        System.out.println("Keyspaces:");
        List<String> keyspaces = connectionManager.getKeyspaces();

        List<Map<String, Object>> rows = new ArrayList<>();
        for (String keyspace : keyspaces) {
            rows.add(Map.of("keyspace_name", keyspace));
        }

        TableFormatter formatter = new TableFormatter(formattingConfig);
        System.out.println(formatter.formatTable(List.of("keyspace_name"), rows));
    }

    private void describeTables(boolean full) {
        String currentKeyspace = connectionManager.getCurrentKeyspace();
        if (currentKeyspace == null || currentKeyspace.isEmpty()) {
            System.out.println("Error: No keyspace selected. Use USE <keyspace> first.");
            return;
        }

        System.out.println("Tables in keyspace " + currentKeyspace + ":");
        List<String> tables = connectionManager.getTables(currentKeyspace);

        List<Map<String, Object>> rows = new ArrayList<>();
        for (String table : tables) {
            rows.add(Map.of("table_name", table));
        }

        TableFormatter formatter = new TableFormatter(formattingConfig);
        System.out.println(formatter.formatTable(List.of("table_name"), rows));
    }

    private void describeTable(String tableName, boolean full) {
        try {
            // Describe table schema using metadata
            String currentKs = connectionManager.getCurrentKeyspace();
            String spec = tableName.contains(".") ? tableName : (currentKs != null ? currentKs + "." + tableName : tableName);
            TableMetadata meta = connectionManager.getTableMetadata(spec);
            System.out.println("Table: " + spec);
            // Columns and types
            System.out.println("Columns:");
            meta.getColumns().forEach((colName, colMeta) -> {
                System.out.printf("  %s %s%s%n", colName,
                    colMeta.getType(), colMeta.isStatic() ? " STATIC" : "");
            });
            // Primary key
            java.util.List<String> pkCols = meta.getPartitionKey().stream()
                .map(c -> c.getName().toString())
                .collect(java.util.stream.Collectors.toList());
            java.util.List<String> ckCols = meta.getClusteringColumns().keySet().stream()
                .map(c -> c.getName().toString())
                .collect(java.util.stream.Collectors.toList());
            String pkDisplay;
            if (ckCols.isEmpty()) {
                pkDisplay = pkCols.size() > 1 ? "(" + String.join(", ", pkCols) + ")" : pkCols.get(0);
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append("(");
                if (pkCols.size() > 1) sb.append("(").append(String.join(", ", pkCols)).append(")"); else sb.append(pkCols.get(0));
                sb.append(", ").append(String.join(", ", ckCols)).append(")");
                pkDisplay = sb.toString();
            }
            System.out.println("Primary key: " + pkDisplay);
            // Optionally show clustering order
            if (!ckCols.isEmpty()) {
                System.out.println("Clustering columns:");
                meta.getClusteringColumns().forEach((c, order) ->
                    System.out.printf("  %s %s%n", c.getName(), order));
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            logger.error("Error describing table", e);
        }
    }

    @Override
    public String getName() {
        return "DESCRIBE";
    }

    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}