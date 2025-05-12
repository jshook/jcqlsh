package org.cqlsh.formatting;

import org.cqlsh.config.FormattingConfig;

import java.util.ArrayList;
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
     */
    public void formatTable(List<String> columns, List<Map<String, Object>> rows) {
