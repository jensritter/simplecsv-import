package org.jens.csvimporter.core;

import java.io.File;
import java.time.LocalDateTime;

/**
 * Allgemeine Informationen über die importierte Datei
 *
 * @author Jens Ritter on 23.08.2024.
 */
public record FileMeta(String path, String filename, LocalDateTime modifed) {
    public String getFullFilename() {return path + File.separator + filename;}
}
