package org.jens.csvimporter.cli.properties;

import org.jens.shorthand.jdbc.JdbcNGConnectionProperty;
import org.jens.shorthand.jdbc.ng.treiber.DbType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
@ConfigurationProperties("csvimporter.target")
public class TargetProperties extends JdbcNGConnectionProperty {

    private DbType type = DbType.H2;

    public DbType getType() {return type;}

    public void setType(DbType type) {this.type = type;}
}