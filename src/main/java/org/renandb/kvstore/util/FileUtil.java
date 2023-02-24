package org.renandb.kvstore.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class FileUtil {

    public static void deleteDir(Path dir) throws IOException {
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    public static Path createTempDir() throws IOException {
        return Files.createTempDirectory("kvstore");
    }
}
