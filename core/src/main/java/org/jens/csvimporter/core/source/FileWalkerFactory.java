package org.jens.csvimporter.core.source;

import org.springframework.stereotype.Service;

/**
 * @author Jens Ritter on 23.08.2024.
 */
@Service
public class FileWalkerFactory {

    public FileContentReader createFileWalker(boolean onlyZip) {
        if (onlyZip) {
            return new FileContentReaderZip(onlyZip);
        } else {
            return new FileContentReaderFile();
        }
    }
}
