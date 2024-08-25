package org.jens.csvimporter.cli.properties;

import org.jens.shorthand.jdbc.JdbcNGConnectionProperty;
import org.jens.shorthand.jdbc.ng.treiber.DbType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Optionen für die Zieldatenbank.
 *
 * @author Jens Ritter on 23.08.2024.
 */
@ConfigurationProperties("target")
public class TargetProperties extends JdbcNGConnectionProperty {

    /**
     * Datenbank Type
     */
    private DbType type;

    /**
     * Prefix für den erstellen Tabellen
     */
    private String prefix;

    public DbType getType() {return type;}

    public void setType(DbType type) {this.type = type;}

    public String getPrefix() {return prefix;}

    public void setPrefix(String prefix) {this.prefix = prefix;}

    @Override
    public String toString() {
        return "TargetProperties{" +
               "type=" + type +
               ", prefix='" + prefix + '\'' +
               "} " + super.toString();
    }
}
