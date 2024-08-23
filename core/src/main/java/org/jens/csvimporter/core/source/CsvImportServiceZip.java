package org.jens.csvimporter.core.source;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.jens.csvimporter.core.FileMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
public class CsvImportServiceZip implements CsvImportService {

    private final boolean onlyZip;

    public CsvImportServiceZip(final boolean onlyZip) {this.onlyZip = onlyZip;}


    @Override
    public int walk(Path base, OnFoundFile finder) throws IOException {
        CsvImportService plain = new CsvImportServiceFile();
        return plain.walk(base, (meta, in)->{
            if (meta.filename().toLowerCase(Locale.ROOT).endsWith(".zip")) {
                parseZip(meta, in, finder);
            } else {
                if (!onlyZip) {
                    finder.onFile(meta, in);
                }
            }
        });
    }

    private final Logger logger = LoggerFactory.getLogger(CsvImportServiceZip.class);

    private void parseZip(FileMeta fileMeta, InputStream in, OnFoundFile finder) throws SQLException {
        String pfad = fileMeta.path() + File.separator + fileMeta.filename();
        try (ZipInputStream zip = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                String name = entry.getName();
                if (logger.isDebugEnabled()) {
                    String cmt = entry.getComment();
                    LocalDateTime timeLocal = entry.getTimeLocal();
                    logger.debug("{}: {} {} {}", fileMeta.path(), name, cmt, timeLocal);
                }
                try (InputStream zipInput = CloseShieldInputStream.wrap(zip);) {
                    finder.onFile(new FileMeta(pfad, name, fileMeta.modifed()), zipInput);
                } catch (IOException e) {
                    logger.error("{}!{} : {}", pfad, name, e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            logger.error("{} : {}", pfad, e.getMessage(), e);
        }
    }
}
