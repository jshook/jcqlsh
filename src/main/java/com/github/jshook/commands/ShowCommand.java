package com.github.jshook.commands;

import com.github.jshook.config.FormattingConfig;
import com.github.jshook.connection.ConnectionManager;
import org.cqlsh.formatting.ResultFormatter;
import com.github.jshook.formatting.TableFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Command that shows information about the Cassandra cluster.
 */
public class ShowCommand implements Command {
    private static final Logger logger = LoggerFactory.getLogger(ShowCommand.class);
    
    private static final String HELP_MESSAGE = 
            "SHOW - Displays information about the Cassandra cluster\n" +
            "Usage: SHOW [TYPE]\n" +
            "Where TYPE is one of: KEYSPACES, DATACENTERS, TABLES, ALL\n" +
            "If TYPE is omitted, ALL is assumed.";
    
    private final ConnectionManager connectionManager;
    private final FormattingConfig formattingConfig;
    
    public ShowCommand(ConnectionManager connectionManager, FormattingConfig formattingConfig) {
        this.connectionManager = connectionManager;
        this.formattingConfig = formattingConfig;
    }
    
    @Override
    public boolean execute(String args) {
        if (args.equalsIgnoreCase("help") || args.equalsIgnoreCase("?")) {
            System.out.println(HELP_MESSAGE);
            return true;
        }
        
        String type = args.trim().toUpperCase();
        
        if (type.isEmpty() || type.equals("ALL")) {
            showAll();
        } else if (type.equals("KEYSPACES")) {
            showKeyspaces();
        } else if (type.equals("DATACENTERS")) {
            showDatacenters();
        } else if (type.equals("TABLES")) {
            showTables();
        } else {
            System.out.println("Unknown show type: " + type);
            System.out.println(HELP_MESSAGE);
        }
        
        return true;
    }
    
    private void showAll() {
        showDatacenters();
        System.out.println();
        showKeyspaces();
        System.out.println();
        showTables();
    }
    
    private void showKeyspaces() {
        System.out.println("Keyspaces:");
        List<String> keyspaces = connectionManager.getKeyspaces();
        
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String keyspace : keyspaces) {
            rows.add(Map.of("keyspace_name", keyspace));
        }
        
        TableFormatter formatter = new TableFormatter(formattingConfig);
        formatter.formatTable(List.of("keyspace_name"), rows);
    }
    
    private void showDatacenters() {
        System.out.println("Datacenters:");
        List<Map<String, String>> datacenters = connectionManager.getDatacenters();
        
        List<Map<String, Object>> rows = datacenters.stream()
                .map(dc -> Map.<String, Object>of(
                        "datacenter", dc.get("name"),
                        "nodes", dc.get("nodeCount")))
                .collect(Collectors.toList());
        
        TableFormatter formatter = new TableFormatter(formattingConfig);
        formatter.formatTable(Arrays.asList("datacenter", "nodes"), rows);
    }
    
    private void showTables() {
        String currentKeyspace = connectionManager.getCurrentKeyspace();
        if (currentKeyspace == null || currentKeyspace.isEmpty()) {
            System.out.println("Tables: No keyspace selected. Use USE <keyspace> first.");
            return;
        }
        
        System.out.println("Tables in keyspace " + currentKeyspace + ":");
        List<String> tables = connectionManager.getTables(currentKeyspace);
        
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String table : tables) {
            rows.add(Map.of("table_name", table));
        }
        
        TableFormatter formatter = new TableFormatter(formattingConfig);
        formatter.formatTable(List.of("table_name"), rows);
    }
    
    @Override
    public String getName() {
        return "SHOW";
    }
    
    @Override
    public String getHelp() {
        return HELP_MESSAGE;
    }
}
