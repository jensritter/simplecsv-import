package org.jens.csvimporter.core;

import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvParserSettings;
import org.jens.csvimporter.core.source.CsvImportService;
import org.jens.csvimporter.core.target.FlexiblePst;
import org.jens.csvimporter.core.target.JdbcImporter;
import org.jens.shorthand.data.reader.UniCsvReader;
import org.jens.shorthand.io.open.EncodingGuesserReader;
import org.jens.shorthand.stringutils.JavaTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
public class CsvImporter {

    private final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    private final CsvParserSettings settings;

    public CsvImporter(char csvDelimeter) {
        settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(csvDelimeter);
        settings.getFormat().setLineSeparator("\n");
    }

    public int doImport(Path sourcePath, CsvImportService importer, JdbcImporter jdbcImporter) throws SQLException, IOException {
        try (Connection con = jdbcImporter.getCon()) {

            var start = LocalDateTime.now();

            int rowcount = importer.walk(sourcePath, (filemeta, in)->{
                logger.debug("{} {}", filemeta, in);

                if (filemeta.filename().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                    long fileid = jdbcImporter.addMeta(con, filemeta);

                    logger.info("Importing {} {} with fileid={}", filemeta.path(), filemeta.filename(), fileid);
                    try {
                        LocalDateTime start1 = LocalDateTime.now();
                        importCsvFile(in, jdbcImporter, con, fileid);
                        logger.info("{} Duration {}", fileid, JavaTimeFormatter.durationPrinterHuman(start1, LocalDateTime.now()));
                    } catch (IOException e) {
                        logger.warn("{} {} {}", filemeta.path(), filemeta.filename(), e.getMessage());
                    }
                } else {
                    logger.info("Ignoring non-csv file {}{}{}", filemeta.path(), File.separator, filemeta.filename());
                }
            });

            logger.info("Complete-Import duration {}", JavaTimeFormatter.durationPrinterHuman(start, LocalDateTime.now()));
            return rowcount;
        }

    }

    private int importCsvFile(InputStream in, JdbcImporter importer, Connection con, long fileid) throws IOException, SQLException {

        BufferedReader bufferedReader = EncodingGuesserReader.guessReader(in);
        AtomicInteger lineCounter = new AtomicInteger(0);

        int columns = 0;
        FlexiblePst pst = null;

        try (UniCsvReader reader = new UniCsvReader(bufferedReader, settings)) {
            List<String> line = reader.getLine();

            if (line == null) {
                logger.info("Skippe Leere Datei");
                return 0;
            }

            do {
                int lineCount = lineCounter.incrementAndGet();
                if (pst == null || line.size() > columns) {
                    pst = importer.prepareInsert(pst, con, fileid, line.size());
                    columns = line.size();
                }

                pst.addLine(lineCount, line);
                line = reader.getLine();
                if ((lineCount % 1_000) == 0) {
                    logger.trace("Imported {} Lines", lineCount);
                }

            } while (line != null);
            pst.close();

        } catch (TextParsingException e) {
            // Passiert z.B. wenn die Zip-Kapuut ist.
            throw new IOException(e);
        }
        return lineCounter.get();
    }
}
