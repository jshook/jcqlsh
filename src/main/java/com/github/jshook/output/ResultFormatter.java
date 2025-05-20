package com.github.jshook.output;

import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Interface for formatting ResultSet data.
 */
public interface ResultFormatter {
    /**
     * Formats a result set into a string representation.
     *
     * @param resultSet the result set to format
     * @return the formatted result
     */
    String format(ResultSet resultSet);
}
