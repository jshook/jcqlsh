package org.cqlsh.output;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.cqlsh.config.FormattingConfig;
import org.cqlsh.config.OutputFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating result formatters based on the formatting configuration.
 */
public class ResultFormatterFactory {
    private final FormattingConfig formattingConfig;
    
    /**
     * Creates a new ResultFormatterFactory with the given formatting configuration.
     *
     * @param formattingConfig the formatting configuration
     */
    public ResultFormatterFactory(FormattingConfig formattingConfig) {
        this.formattingConfig = formattingConfig;
    }
    
    /**
     * Creates a formatter for the given result set based on the formatting configuration.
     *
     * @param resultSet the result set to format
     * @return the appropriate formatter
     */
    public ResultFormatter createFormatter(ResultSet resultSet) {
        if (formattingConfig.getOutputFormat() == OutputFormat.JSON) {
            return new JsonResultFormatter();
        } else if (formattingConfig.getOutputFormat() == OutputFormat.CSV) {
            return new CsvResultFormatter();
        } else {
            return new TabularResultFormatter(formattingConfig);
        }
    }
    
    /**
     * Tabular result formatter for displaying results in a table format.
     */
    private static class TabularResultFormatter implements ResultFormatter {
        private final FormattingConfig config;
        
        public TabularResultFormatter(FormattingConfig config) {
            this.config = config;
        }
        
        @Override
        public String format(ResultSet resultSet) {
            if (resultSet == null || !resultSet.iterator().hasNext()) {
                return "(No rows)";
            }
            
            StringBuilder sb = new StringBuilder();
            
            // Get column definitions
            var columnDefs = resultSet.getColumnDefinitions();
            int numColumns = columnDefs.size();
            
            // If expanded format is enabled, show each row vertically
            if (config.isExpandedFormat()) {
                int rowCount = 0;
                for (var row : resultSet) {
                    rowCount++;
                    sb.append("@ Row ").append(rowCount).append("\n");
                    sb.append("-------------\n");
                    
                    for (int i = 0; i < numColumns; i++) {
                        String name = columnDefs.get(i).getName().toString();
                        String value = row.isNull(i) ? "null" : row.getObject(i).toString();
                        sb.append(name).append(": ").append(value).append("\n");
                    }
                    sb.append("\n");
                }
            } else {
                // Regular tabular output
                // Determine column widths
                int[] columnWidths = new int[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnWidths[i] = columnDefs.get(i).getName().toString().length();
                }
                
                // Collect all rows to determine optimal column widths
                var rows = resultSet.all();
                for (var row : rows) {
                    for (int i = 0; i < numColumns; i++) {
                        if (!row.isNull(i)) {
                            String value = row.getObject(i).toString();
                            columnWidths[i] = Math.max(columnWidths[i], value.length());
                        }
                    }
                }
                
                // Adjust widths to max width
                for (int i = 0; i < columnWidths.length; i++) {
                    columnWidths[i] = Math.min(columnWidths[i], config.getMaxWidth() / numColumns);
                }
                
                // Print header
                for (int i = 0; i < numColumns; i++) {
                    String name = columnDefs.get(i).getName().toString();
                    sb.append(padRight(name, columnWidths[i]));
                    sb.append(" | ");
                }
                sb.append("\n");
                
                // Print separator
                for (int i = 0; i < numColumns; i++) {
                    for (int j = 0; j < columnWidths[i]; j++) {
                        sb.append("-");
                    }
                    sb.append("-+-");
                }
                sb.append("\n");
                
                // Print rows
                for (var row : rows) {
                    for (int i = 0; i < numColumns; i++) {
                        String value = row.isNull(i) ? "null" : row.getObject(i).toString();
                        // Truncate value if it's too long
                        if (value.length() > columnWidths[i]) {
                            value = value.substring(0, columnWidths[i] - 3) + "...";
                        }
                        sb.append(padRight(value, columnWidths[i]));
                        sb.append(" | ");
                    }
                    sb.append("\n");
                }
            }
            
            return sb.toString();
        }
        
        /**
         * Pads a string on the right with spaces to the given width.
         *
         * @param s     the string to pad
         * @param width the desired width
         * @return the padded string
         */
        private String padRight(String s, int width) {
            if (s.length() >= width) {
                return s;
            }
            StringBuilder sb = new StringBuilder(s);
            while (sb.length() < width) {
                sb.append(" ");
            }
            return sb.toString();
        }
    }
    
    /**
     * JSON result formatter for displaying results in JSON format.
     */
    private static class JsonResultFormatter implements ResultFormatter {
        private final ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public String format(ResultSet resultSet) {
            if (resultSet == null || !resultSet.iterator().hasNext()) {
                return "[]";
            }
            
            try {
                ArrayNode rootArray = objectMapper.createArrayNode();
                var columnDefs = resultSet.getColumnDefinitions();
                
                for (Row row : resultSet) {
                    ObjectNode rowNode = objectMapper.createObjectNode();
                    
                    for (int i = 0; i < columnDefs.size(); i++) {
                        String name = columnDefs.get(i).getName().toString();
                        if (row.isNull(i)) {
                            rowNode.putNull(name);
                        } else {
                            // This is a simplification - in a real implementation,
                            // we would need to handle different data types properly
                            rowNode.put(name, row.getObject(i).toString());
                        }
                    }
                    
                    rootArray.add(rowNode);
                }
                
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootArray);
            } catch (JsonProcessingException e) {
                return "Error formatting JSON: " + e.getMessage();
            }
        }
    }
    
    /**
     * CSV result formatter for displaying results in CSV format.
     */
    private static class CsvResultFormatter implements ResultFormatter {
        @Override
        public String format(ResultSet resultSet) {
            if (resultSet == null || !resultSet.iterator().hasNext()) {
                return "";
            }
            
            var columnDefs = resultSet.getColumnDefinitions();
            int numColumns = columnDefs.size();
            
            // Extract column names
            String[] headers = new String[numColumns];
            for (int i = 0; i < numColumns; i++) {
                headers[i] = columnDefs.get(i).getName().toString();
            }
            
            StringWriter writer = new StringWriter();
            try (CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headers))) {
                for (Row row : resultSet) {
                    List<Object> rowData = new ArrayList<>();
                    for (int i = 0; i < numColumns; i++) {
                        rowData.add(row.isNull(i) ? null : row.getObject(i));
                    }
                    csvPrinter.printRecord(rowData);
                }
                
                return writer.toString();
            } catch (IOException e) {
                return "Error formatting CSV: " + e.getMessage();
            }
        }
    }
}
