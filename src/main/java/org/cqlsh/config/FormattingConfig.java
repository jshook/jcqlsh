package org.cqlsh.config;

/**
 * Configuration for formatting query results.
 */
public class FormattingConfig {
    private OutputFormat outputFormat;
    private boolean expandedFormat;
    private int maxWidth;
    private boolean colorEnabled;

    /**
     * Creates a new FormattingConfig with the specified settings.
     *
     * @param outputFormat   the output format to use
     * @param expandedFormat whether to use expanded format for tabular output
     * @param maxWidth       the maximum width for tabular output
     * @param colorEnabled   whether to enable colored output
     */
    public FormattingConfig(OutputFormat outputFormat, boolean expandedFormat, int maxWidth, boolean colorEnabled) {
        this.outputFormat = outputFormat;
        this.expandedFormat = expandedFormat;
        this.maxWidth = maxWidth;
        this.colorEnabled = colorEnabled;
    }

    /**
     * Gets the output format.
     *
     * @return the output format
     */
    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    /**
     * Sets the output format.
     *
     * @param outputFormat the output format
     */
    public void setOutputFormat(OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    /**
     * Checks if expanded format is enabled.
     *
     * @return true if expanded format is enabled, false otherwise
     */
    public boolean isExpandedFormat() {
        return expandedFormat;
    }

    /**
     * Sets whether expanded format is enabled.
     *
     * @param expandedFormat true to enable expanded format, false to disable it
     */
    public void setExpandedFormat(boolean expandedFormat) {
        this.expandedFormat = expandedFormat;
    }

    /**
     * Gets the maximum width for tabular output.
     *
     * @return the maximum width
     */
    public int getMaxWidth() {
        return maxWidth;
    }

    /**
     * Sets the maximum width for tabular output.
     *
     * @param maxWidth the maximum width
     */
    public void setMaxWidth(int maxWidth) {
        this.maxWidth = maxWidth;
    }

    /**
     * Checks if colored output is enabled.
     *
     * @return true if colored output is enabled, false otherwise
     */
    public boolean isColorEnabled() {
        return colorEnabled;
    }

    /**
     * Sets whether colored output is enabled.
     *
     * @param colorEnabled true to enable colored output, false to disable it
     */
    public void setColorEnabled(boolean colorEnabled) {
        this.colorEnabled = colorEnabled;
    }
}
