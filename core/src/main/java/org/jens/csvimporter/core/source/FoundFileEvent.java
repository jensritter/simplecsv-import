package org.jens.csvimporter.core.source;

import org.jens.csvimporter.core.FileMeta;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * @author Jens Ritter on 23.08.2024.
 */
public
interface FoundFileEvent {
    long onFile(FileMeta filemeta, InputStream in) throws SQLException, IOException;
}
