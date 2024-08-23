package org.jens.csvimporter.core.source;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.jens.csvimporter.core.FileMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Jens Ritter on 23.08.2024.
 */
public class FileContentReaderZip implements FileContentReader {
    private final Logger logger = LoggerFactory.getLogger(FileContentReaderZip.class);

    private final boolean filesAlso;

    FileContentReaderZip(final boolean onlyZip) {this.filesAlso = !onlyZip;}


    private static boolean isZipFile(String filename) {
        return filename.toLowerCase(Locale.ROOT).endsWith(".zip");
    }

    private static boolean isCsvFile(String filename) {
        return filename.toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    @Override
    public long walk(Path base, FoundFileEvent finder) throws IOException {
        FileContentReader plain = new FileContentReaderFile();
        return plain.walk(base, (meta, in)->{
            if (isZipFile(meta.filename())) {
                return parseZip(meta, in, finder);
            } else {
                if (filesAlso) {
                    if (isCsvFile(meta.filename())) {
                        return finder.onFile(meta, in);
                    } else {
                        logger.info("Ignoring Non-Csv '{}'", meta.getFullFilename());
                    }
                } else {
                    logger.debug("Ignoring Non-Zip '{}'", meta.getFullFilename());
                }
            }
            return 0;
        });
    }

    private long parseZip(FileMeta fileMeta, InputStream in, FoundFileEvent finder) throws SQLException {

        String zipfile = fileMeta.path() + "!" + fileMeta.filename();

        long rowcount = 0;
        try (ZipInputStream zip = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                String name = entry.getName();

                if (logger.isDebugEnabled()) {
                    String cmt = entry.getComment();
                    LocalDateTime timeLocal = entry.getTimeLocal();
                    logger.debug("{}: {} {} {}", fileMeta.path(), name, cmt, timeLocal);
                }
                if (isCsvFile(name)) {
                    try (InputStream zipInput = CloseShieldInputStream.wrap(zip);) {
                        rowcount += finder.onFile(new FileMeta(zipfile, name, fileMeta.modifed()), zipInput);
                    } catch (IOException e) {
                        logger.error("{}!{} : {}", zipfile, name, e.getMessage(), e);
                    }
                } else {
                    logger.info("Ignoring Non-Csv '{}' in Zipfipe '{}'", name, zipfile);
                }
            }
        } catch (IOException e) {
            logger.error("{} : {}", zipfile, e.getMessage(), e);
        }
        return rowcount;
    }
}
