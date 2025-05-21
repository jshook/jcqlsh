package com.github.jshook.formatting;

import com.github.jshook.config.FormattingConfig;

import java.util.List;
import java.util.Map;

/**
 * Formats data as a table for display.
 */
public class TableFormatter {
    private final FormattingConfig config;
    
    public TableFormatter(FormattingConfig config) {
        this.config = config;
    }
    
    /**
     * Formats a list of rows as a table.
     * @param columns the column names
     * @param rows the rows of data
     * @return the formatted table as a string
     */
    public String formatTable(List<String> columns, List<Map<String, Object>> rows) {
        if (columns == null || columns.isEmpty() || rows == null || rows.isEmpty()) {
            return "(No rows)";
        }
        
        StringBuilder sb = new StringBuilder();
        
        // If expanded format is enabled, show each row vertically
        if (config.isExpandedFormat()) {
            for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                Map<String, Object> row = rows.get(rowIndex);
                sb.append("@ Row ").append(rowIndex + 1).append("\n");
                sb.append("-------------\n");
                
                for (String column : columns) {
                    Object value = row.get(column);
                    String valueStr = value == null ? "null" : value.toString();
                    sb.append(column).append(": ").append(valueStr).append("\n");
                }
                sb.append("\n");
            }
        } else {
            // Regular tabular output
            int numColumns = columns.size();
            
            // Determine column widths
            int[] columnWidths = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columnWidths[i] = columns.get(i).length();
            }
            
            // Calculate optimal column widths based on data
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < numColumns; i++) {
                    String column = columns.get(i);
                    Object value = row.get(column);
                    if (value != null) {
                        String valueStr = value.toString();
                        columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
                    }
                }
            }
            
            // Adjust widths to max width
            int maxColumnWidth = config.getMaxWidth() / numColumns;
            for (int i = 0; i < columnWidths.length; i++) {
                columnWidths[i] = Math.min(columnWidths[i], maxColumnWidth);
            }
            
            // Print header
            for (int i = 0; i < numColumns; i++) {
                sb.append(padRight(columns.get(i), columnWidths[i]));
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
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < numColumns; i++) {
                    String column = columns.get(i);
                    Object value = row.get(column);
                    String valueStr = value == null ? "null" : value.toString();
                    
                    // Truncate value if it's too long
                    if (valueStr.length() > columnWidths[i]) {
                        valueStr = valueStr.substring(0, columnWidths[i] - 3) + "...";
                    }
                    
                    sb.append(padRight(valueStr, columnWidths[i]));
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