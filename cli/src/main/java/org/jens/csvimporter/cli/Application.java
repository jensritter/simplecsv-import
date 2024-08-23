package org.jens.csvimporter.cli;

import org.jens.csvimporter.cli.properties.SourceProperties;
import org.jens.csvimporter.cli.properties.TargetProperties;
import org.jens.csvimporter.core.CsvImporter;
import org.jens.csvimporter.core.source.CsvImportService;
import org.jens.csvimporter.core.source.SourceFactory;
import org.jens.csvimporter.core.target.JdbcImporter;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.spring.boot.HostnameAwareSpringApplicationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
@SpringBootApplication
@EnableConfigurationProperties({SourceProperties.class, TargetProperties.class})
public class Application implements ApplicationRunner, ExitCodeGenerator {
    public static void main(String[] args) {
        new HostnameAwareSpringApplicationBuilder(Application.class)
            .web(WebApplicationType.NONE)
            .run(args);
    }


    private final Logger logger = LoggerFactory.getLogger(Application.class);
    @Autowired
    private SourceProperties sourceProperties;
    @Autowired
    private TargetProperties targetProperties;

    @Autowired
    private SourceFactory sourceFactory;

    private int exitCode = 1;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("Programm-Arguments : {}", args);
        logger.info("{}", sourceProperties);
        logger.info("{}", targetProperties);

        CsvImportService importer = sourceFactory.createFileImporter(sourceProperties.isHandleZip());
        Path sourcePath = Path.of(sourceProperties.getDirname());
        if (!Files.exists(sourcePath)) {
            logger.error("'{}' nicht vorhanden.", sourcePath);
            exitCode = 255;
            return;
        }


        final JdbcNG ng = buildNg();
        JdbcImporter jdbcImporter = new JdbcImporter(ng);
        var csvImporter = new CsvImporter(';');
        int rowcount = csvImporter.doImport(sourcePath, importer, jdbcImporter);

        NumberFormat integerInstance = NumberFormat.getIntegerInstance();
        integerInstance.setGroupingUsed(true);
        logger.info("{} rows inserted", integerInstance.format(rowcount));
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    private JdbcNG buildNg() {
        var builder = switch (targetProperties.getType()) {
            case H2 -> JdbcNG.h2();
            case POSTGRES -> JdbcNG.postgres();
            case MSSQL -> JdbcNG.mssql();
            case MYSQL -> JdbcNG.mysql();
            case MARIADB -> JdbcNG.mariadb();
            case HSQL -> JdbcNG.hsql();
            case ORACLE -> JdbcNG.oracle();
            case DERBY -> JdbcNG.derby();
            case SQLITE -> JdbcNG.sqlite();
            case HSQLFILE -> JdbcNG.hsqlFile();
            case DERBYFILE -> JdbcNG.derbyFile();
            case SQLITEFILE -> JdbcNG.sqliteFile();
            case H2FILE -> JdbcNG.h2File();
            default -> throw new IllegalStateException("unimplemented: ");
        };
        return builder
            .fromProperties(targetProperties);
    }
}