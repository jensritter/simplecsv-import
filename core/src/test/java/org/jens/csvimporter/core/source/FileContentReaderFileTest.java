package org.jens.csvimporter.core.source;

import org.jens.shorthand.io.IOHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Jens Ritter on 24.08.2024.
 */
class FileContentReaderFileTest {

    @Test
    void walk(@TempDir Path tempDir) throws IOException {
        var someFile = Path.of(tempDir.toString(), "someFile.txt");
        Files.writeString(someFile, "1");

        var someDir = Path.of(tempDir.toString(), "someDir");
        Files.createDirectory(someDir);

        var someFileInDir = Path.of(someDir.toString(), "otherFile.csv");
        Files.writeString(someFileInDir, "2");

        FileContentReaderFile fileContentReaderFile = new FileContentReaderFile();

        long count = fileContentReaderFile.walk(tempDir, (filemeta, in)->{
            String fileContent = IOHelper.getStringFromInputStream(in, StandardCharsets.UTF_8);
            return Integer.parseInt(fileContent);
        });
        assertThat(count).isEqualTo(1 + 2); // 2 Files
    }
}
