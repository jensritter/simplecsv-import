package org.jens.csvimporter.core.source;

import org.jens.csvimporter.core.FileMeta;
import org.jens.shorthand.stringutils.JavaTimeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
public class CsvImportServiceFile implements CsvImportService {

    private final Logger logger = LoggerFactory.getLogger(CsvImportServiceFile.class);

    @Override
    public int walk(Path base, OnFoundFile finder) throws IOException {

        AtomicInteger fileCounter = new AtomicInteger(0);
        Files.walkFileTree(base, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                File file = path.toFile();

                String dir = file.getParentFile().toString();
                String name = file.getName();

                var modifed = attrs.lastModifiedTime();
                var ldtModified = JavaTimeHelper.filetime2LocalDateTime(modifed);

                try (FileInputStream fis = new FileInputStream(file);) {
                    fileCounter.incrementAndGet();
                    finder.onFile(
                        new FileMeta(dir, name, ldtModified),
                        fis
                    );
                    return FileVisitResult.CONTINUE;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                logger.warn("{} {}", file.toString(), exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });
        return fileCounter.intValue();
    }
}
