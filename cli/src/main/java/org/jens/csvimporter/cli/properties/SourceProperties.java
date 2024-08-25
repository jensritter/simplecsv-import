package org.jens.csvimporter.cli.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Optionen für die Dateisuche.
 *
 * @author Jens Ritter on 23.08.2024.
 */
@ConfigurationProperties("source")
public class SourceProperties {
    /**
     * Verzeichnis, in dem nach Dateien gesucht werden soll.
     * <p>
     * Es werden autom. auch alle Unterverzeichnisse durchsucht.
     */
    private String dirname;

    /**
     * Sollen nur .zip-Dateien für den CSV-Import verwendet werden?
     * <p>
     * Entpackte CSV-Dateien im Verzeichnis werden dann ignoriert.
     */
    private boolean handleOnlyZip = false;

    /**
     * Welcher Trenner wird für die CSV-Dateien verwendet?
     */
    private char csvdelimiter = ';';

    public String getDirname() {return dirname;}

    public void setDirname(String dirname) {this.dirname = dirname;}

    public boolean isHandleOnlyZip() {return handleOnlyZip;}

    public void setHandleOnlyZip(boolean handleOnlyZip) {this.handleOnlyZip = handleOnlyZip;}

    public char getCsvdelimiter() {return csvdelimiter;}

    public void setCsvdelimiter(char csvdelimiter) {this.csvdelimiter = csvdelimiter;}

    @Override
    public String toString() {
        return "SourceProperties{" +
               "dirname='" + dirname + '\'' +
               ", handleZip=" + handleOnlyZip +
               ", csvdelimiter=" + csvdelimiter +
               '}';
    }
}
