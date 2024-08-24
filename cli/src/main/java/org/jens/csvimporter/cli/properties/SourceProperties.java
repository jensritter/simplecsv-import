package org.jens.csvimporter.cli.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@ConfigurationProperties("csvimporter.source")
public class SourceProperties {
    private String dirname;

    private boolean handleZip = true;

    private char csvdelimiter = ',';

    public String getDirname() {return dirname;}

    public void setDirname(String dirname) {this.dirname = dirname;}

    public boolean isHandleZip() {return handleZip;}

    public void setHandleZip(boolean handleZip) {this.handleZip = handleZip;}

    public char getCsvdelimiter() {return csvdelimiter;}

    public void setCsvdelimiter(char csvdelimiter) {this.csvdelimiter = csvdelimiter;}

    @Override
    public String toString() {
        return "SourceProperties{" +
               "dirname='" + dirname + '\'' +
               ", handleZip=" + handleZip +
               ", csvdelimiter=" + csvdelimiter +
               '}';
    }
}
