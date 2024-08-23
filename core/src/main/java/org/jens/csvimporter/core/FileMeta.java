package org.jens.csvimporter.core;

import java.io.File;
import java.time.LocalDateTime;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
public record FileMeta(String path, String filename, LocalDateTime modifed) {
    public String getFullFilename() {return path + File.separator + filename;}
}
