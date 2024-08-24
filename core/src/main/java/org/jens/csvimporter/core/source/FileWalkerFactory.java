package org.jens.csvimporter.core.source;

/**
 * @author Jens Ritter on 23.08.2024.
 */
public class FileWalkerFactory {

    public FileContentReader createFileWalker(boolean onlyZip) {
        if (onlyZip) {
            return new FileContentReaderZip(onlyZip);
        } else {
            return new FileContentReaderFile();
        }
    }
}
