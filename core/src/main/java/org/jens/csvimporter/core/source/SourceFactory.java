package org.jens.csvimporter.core.source;

import org.springframework.stereotype.Service;

/**
 * @author CsvImporter Ritter on 23.08.2024.
 */
@Service
public class SourceFactory {

    public CsvImportService createFileImporter(boolean zip) {
        if (zip) {
            return new CsvImportServiceZip(true);
        } else {
            return new CsvImportServiceFile();
        }
    }
}
