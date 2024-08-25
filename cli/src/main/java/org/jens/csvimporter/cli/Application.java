package org.jens.csvimporter.cli;

import org.jens.csvimporter.cli.properties.SourceProperties;
import org.jens.csvimporter.cli.properties.TargetProperties;
import org.jens.csvimporter.core.CsvImporter;
import org.jens.csvimporter.core.source.FileContentReader;
import org.jens.csvimporter.core.source.FileContentReaderZip;
import org.jens.csvimporter.core.target.JdbcImporter;
import org.jens.shorthand.jdbc.ng.JdbcNG;
import org.jens.shorthand.spring.boot.ApplicationHelpPrinter;
import org.jens.shorthand.spring.boot.HostnameAwareSpringApplicationBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.autoconfigure.endpoint.jackson.JacksonEndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.configurationmetadata.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@SpringBootApplication
@EnableConfigurationProperties({SourceProperties.class, TargetProperties.class})
public class Application implements ApplicationRunner, ExitCodeGenerator {
    @Autowired
    private JacksonEndpointAutoConfiguration jacksonEndpointAutoConfiguration;

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

    private int exitCode = 1;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        var argv = args.getSourceArgs();
        logger.info("ARGV {}", Arrays.asList(argv));
        logger.info("{}", sourceProperties);
        logger.info("{}", targetProperties);

        if (args.getOptionNames().contains("help")) {
            new ApplicationHelpPrinter()
                .help(args, System.out);
            return;
        }

        List<String> nonOptionArgs = args.getNonOptionArgs();
        Path sourcePath;
        if (!nonOptionArgs.isEmpty()) {
            if (nonOptionArgs.size() != 1) {
                this.exitCode = 255;
                logger.error("Multiple Input-Verzeichnisse sind nicht unterstützt.");
                return;
            }
            sourcePath = Path.of(nonOptionArgs.get(0));
        } else {
            sourcePath = Path.of(sourceProperties.getDirname());
        }


        boolean onlyZip = sourceProperties.isHandleOnlyZip();
        FileContentReader csvReader = new FileContentReaderZip(onlyZip);
        if (!Files.exists(sourcePath)) {
            logger.error("Basisverzeichnis '{}' nicht vorhanden.", sourcePath);
            exitCode = 255;
            return;
        }

        if (targetProperties.getType() == null) {
            logger.error("Kein Datenbanktype ausgewählt. Fehlt die application.properties-Datei ? ");
            exitCode = 255;
            return;
        }

        final JdbcNG ng = buildNg();
        var jdbcImporter = new JdbcImporter(ng, targetProperties.getPrefix());
        var csvImporter = new CsvImporter(sourceProperties.getCsvdelimiter());
        long rowcount = csvImporter.doImport(sourcePath, csvReader, jdbcImporter);

        NumberFormat integerInstance = NumberFormat.getNumberInstance();
        integerInstance.setGroupingUsed(true);
        integerInstance.setMinimumFractionDigits(0);
        logger.info("{} rows inserted", integerInstance.format(rowcount));
        this.exitCode = 0;
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

    private void displayHelp() {
        Map<String, List<ConfigurationMetadataProperty>> jens = new TreeMap<>();
        List<ConfigurationMetadataSource> groupInfo = new ArrayList<>();
        try {
            Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:META-INF/spring-configuration-metadata.json");
            ConfigurationMetadataRepositoryJsonBuilder builder = ConfigurationMetadataRepositoryJsonBuilder.create();
            for (Resource resource : resources) {
                try (InputStream in = resource.getInputStream()) {
                    builder.withJsonResource(in);
                }
            }

            ConfigurationMetadataRepository repo = builder.build();

            Map<String, ConfigurationMetadataGroup> groups = new TreeMap<>(repo.getAllGroups());


            for (Map.Entry<String, ConfigurationMetadataGroup> entry : groups.entrySet()) { // alle gruppen durchgehen ...
                String someKey = entry.getKey();
                if (someKey.startsWith("spring.")) {continue;}
                if (someKey.startsWith("management.")) {continue;}
                if (someKey.startsWith("server.")) {continue;}

                if (someKey.equals("server")) {continue;}
//                if (someKey.equals("_ROOT_GROUP_")) {continue;}
                if (someKey.equals("logging")) {continue;}

                logger.trace("GroupID: {}", someKey);

                Map<String, ConfigurationMetadataSource> sources = entry.getValue().getSources();
                for (Map.Entry<String, ConfigurationMetadataSource> stringConfigurationMetadataSourceEntry : sources.entrySet()) {
                    groupInfo.add(stringConfigurationMetadataSourceEntry.getValue());
                }
            }


            for (ConfigurationMetadataSource group : groupInfo) {
                System.out.println("#" + group.getGroupId() + "#");

                group.getProperties().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(it->{
                    var prop = it.getValue();

                    List<String> desc = new ArrayList<>();
                    if (prop.getShortDescription() != null) {
                        desc.add(" " + prop.getShortDescription());
                    }
                    if (prop.getDescription() != null) {
                        desc.add("\t" + prop.getDescription());
                    }

                    System.out.print("--" + prop.getId());
                    if (prop.getDefaultValue() != null) {
                        System.out.println("=" + prop.getDefaultValue());
                    } else {
                        System.out.println();
                    }

                    if (!desc.isEmpty()) {
                        System.out.println(String.join("\n", desc));
                    } else {
                        System.out.println();
                    }

                });
            }

        } catch (IOException e) {
            throw new IllegalStateException("unimplemented: ");
        }
    }

    private void printHelp(ApplicationArguments args, PrintStream out) {
        List<String> strings = buildHelp();
        out.print(String.join("\n", strings));
    }

    private List<String> buildHelp() {

        List<String> result = new ArrayList<>();

        Map<String, List<ConfigurationMetadataProperty>> jens = new TreeMap<>();
        List<ConfigurationMetadataSource> groupInfo = new ArrayList<>();
        try {
            Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:META-INF/spring-configuration-metadata.json");
            ConfigurationMetadataRepositoryJsonBuilder builder = ConfigurationMetadataRepositoryJsonBuilder.create();
            for (Resource resource : resources) {
                try (InputStream in = resource.getInputStream()) {
                    builder.withJsonResource(in);
                }
            }

            ConfigurationMetadataRepository repo = builder.build();

            Map<String, ConfigurationMetadataGroup> groups = new TreeMap<>(repo.getAllGroups());


            for (Map.Entry<String, ConfigurationMetadataGroup> entry : groups.entrySet()) { // alle gruppen durchgehen ...
                String someKey = entry.getKey();
                if (someKey.startsWith("spring.")) {continue;}
                if (someKey.startsWith("management.")) {continue;}
                if (someKey.startsWith("server.")) {continue;}

                if (someKey.equals("server")) {continue;}
//                if (someKey.equals("_ROOT_GROUP_")) {continue;}
                if (someKey.equals("logging")) {continue;}

                logger.trace("GroupID: {}", someKey);

                Map<String, ConfigurationMetadataSource> sources = entry.getValue().getSources();
                for (Map.Entry<String, ConfigurationMetadataSource> stringConfigurationMetadataSourceEntry : sources.entrySet()) {
                    groupInfo.add(stringConfigurationMetadataSourceEntry.getValue());
                }
            }


            for (ConfigurationMetadataSource group : groupInfo) {
                result.add("# Gruppe " + group.getGroupId() + " #");

                group.getProperties().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(it->{
                    var prop = it.getValue();

                    StringBuilder line = new StringBuilder();
                    line.append("--" + prop.getId());
                    if (prop.getDefaultValue() != null) {
                        line.append("=" + prop.getDefaultValue());
                    } else {
                        line.append("=VALUE");
                    }
                    String type = filterType(prop);
                    if (type != null) {
                        line.append(" ").append(type);
                    }
                    result.add(line.toString());


                    List<String> desc = new ArrayList<>();
                    if (prop.getShortDescription() != null) {
                        desc.add(" " + prop.getShortDescription());
                    }
                    if (prop.getDescription() != null) {

                        var list = Arrays.stream(prop.getDescription().split("\\<p\\>"))
                            .map(x->"\t" + x.trim())
                            .skip(1) // 1. zeile Skippen - das ist bereits in der short-description vorhanden.
                            .toList();
                        desc.addAll(list);
                    }
                    result.addAll(desc);
                });
                result.add(""); // empty line
            }

            return result;

        } catch (IOException e) {
            throw new IllegalStateException("unimplemented: ");
        }
    }

    @Nullable
    private String filterType(ConfigurationMetadataProperty prop) {
        String type = prop.getType();
        if (type.startsWith("java.lang")) {
            return null;
        }

        try {
            Class<?> aClass = Class.forName(type);
            if (aClass.isEnum()) {

                Collection<String> values = new ArrayList<>();
                Field[] fields = aClass.getDeclaredFields();
                for (Field field : fields) {
                    if (field.isEnumConstant()) {
                        Object o = field.get(null);
                        values.add(o.toString());
                    }
                }
                return "Options=" + String.join(",", values);
            }
        } catch (ClassNotFoundException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
        return "type: " + type;
    }


}
