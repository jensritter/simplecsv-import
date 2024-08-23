package org.jens.csvimporter.core;

import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvParserSettings;
import org.jens.csvimporter.core.source.FileContentReader;
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
 * Verbindungs-Haupt-Klasse:
 * <p>
 * Holt sich die Dateien vom FileContentReader und lässt den JdbcImporter die Datensätze speichern.
 *
 * @author Jens Ritter on 23.08.2024.
 */
public class CsvImporter {

    private final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    private final CsvParserSettings settings;

    public CsvImporter(char csvDelimeter) {
        settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(csvDelimeter);
        settings.getFormat().setLineSeparator("\n");
    }

    public long doImport(Path sourcePath, FileContentReader importer, JdbcImporter jdbcImporter) throws SQLException, IOException {
        try (Connection con = jdbcImporter.getCon()) {

            var start = LocalDateTime.now();

            long rowcount = importer.walk(sourcePath, (filemeta, in)->{
                logger.debug("{} {}", filemeta, in);
                if (!filemeta.filename().toLowerCase(Locale.ROOT).endsWith(".csv")) {
                    logger.info("Ignoring non-csv file {}{}{}", filemeta.path(), File.separator, filemeta.filename());
                    return 0;
                }
                long fileid = jdbcImporter.addMeta(con, filemeta);
                logger.info("'{}' fileid:{}", filemeta.getFullFilename(), fileid);
                long imported = 0;
                try {
                    LocalDateTime start1 = LocalDateTime.now();
                    imported = importCsvFile(in, jdbcImporter, con, fileid);
                    logger.info("'{}' duration:{}", filemeta.getFullFilename(), JavaTimeFormatter.durationPrinterHuman(start1, LocalDateTime.now()));
                } catch (IOException e) {
                    logger.warn("{} {}", filemeta.getFullFilename(), e.getMessage());
                }
                return imported;
            });

            logger.info("Complete-Import Duration {}", JavaTimeFormatter.durationPrinterHuman(start, LocalDateTime.now()));
            return rowcount;
        }
    }

    private long importCsvFile(InputStream in, JdbcImporter importer, Connection con, long fileid) throws IOException, SQLException {

        int columns = 0;
        FlexiblePst pst = null;

        AtomicInteger lineCounter = new AtomicInteger(0);
        try (
            BufferedReader bufferedReader = EncodingGuesserReader.guessReader(in);
            UniCsvReader reader = new UniCsvReader(bufferedReader, settings)
        ) {
            List<String> line = reader.getLine();
            if (line == null) {
                logger.info("Skippe Leere Datei");
                return 0;
            }

            do {
                int lineCount = lineCounter.incrementAndGet();
                if (pst == null || line.size() > columns) {
                    if (pst != null) {
                        logger.info("Anzahl der Spalten hat sich verändert: von {} nach {}", columns, line.size());
                    }
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
