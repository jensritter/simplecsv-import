package org.jens.csvimporter.core.source;

import org.jens.csvimporter.core.FileMeta;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.SQLException;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
public interface CsvImportService {

    int walk(Path base, OnFoundFile finder) throws IOException;

    public interface OnFoundFile {
        void onFile(FileMeta filemeta, InputStream in) throws SQLException, IOException;
    }

}
