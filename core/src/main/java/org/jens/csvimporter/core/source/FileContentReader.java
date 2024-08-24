package org.jens.csvimporter.core.source;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@FunctionalInterface
public interface FileContentReader {
    long walk(Path base, FoundCsvFileEvent finder) throws IOException;
}
