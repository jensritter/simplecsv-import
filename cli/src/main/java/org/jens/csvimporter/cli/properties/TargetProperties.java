package org.jens.csvimporter.cli.properties;

import org.jens.shorthand.jdbc.JdbcNGConnectionProperty;
import org.jens.shorthand.jdbc.ng.treiber.DbType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@ConfigurationProperties("csvimporter.target")
public class TargetProperties extends JdbcNGConnectionProperty {

    private DbType type = DbType.H2;

    private String prefix = "JENS";

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
