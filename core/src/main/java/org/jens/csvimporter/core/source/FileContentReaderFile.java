package org.jens.csvimporter.core.source;

import org.jens.csvimporter.core.FileMeta;
import org.jens.shorthand.stringutils.JavaTimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Durchsucht einfach nur ein Verzeichnis, öffnet alle Dateien zum Lesen, und übergibt dies an die FoundCsvFileEvent-Methode
 *
 * @author Jens Ritter on 23.08.2024.
 */
class FileContentReaderFile implements FileContentReader {

    private final Logger logger = LoggerFactory.getLogger(FileContentReaderFile.class);

    @Override
    public long walk(Path base, FoundCsvFileEvent finder) throws IOException {

        AtomicLong fileCounter = new AtomicLong(0);
        Files.walkFileTree(base, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                File file = path.toFile();

                String dir = file.getParentFile().toString();
                String name = file.getName();

                var modifed = attrs.lastModifiedTime();
                var ldtModified = JavaTimeHelper.filetime2LocalDateTime(modifed);

                try (InputStream fis = new FileInputStream(file)) {
                    var rows = finder.onFile(
                        new FileMeta(dir, name, ldtModified),
                        fis
                    );
                    fileCounter.addAndGet(rows);
                    return FileVisitResult.CONTINUE;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                logger.warn("{} {}", file.toString(), exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });
        return fileCounter.longValue();
    }
}
