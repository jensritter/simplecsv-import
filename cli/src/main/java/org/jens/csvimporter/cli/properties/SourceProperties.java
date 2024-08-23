package org.jens.csvimporter.cli.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@ConfigurationProperties("csvimporter.source")
public class SourceProperties {
    private String dirname;

    private boolean handleZip = true;

    public String getDirname() {return dirname;}

    public void setDirname(String dirname) {this.dirname = dirname;}

    public boolean isHandleZip() {return handleZip;}

    public void setHandleZip(boolean handleZip) {this.handleZip = handleZip;}

    @Override
    public String toString() {
        return "SourceProperties{" +
               "dirname='" + dirname + '\'' +
               ", handleZip=" + handleZip +
               '}';
    }
}
