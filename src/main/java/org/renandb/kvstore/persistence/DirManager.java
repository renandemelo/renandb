package org.renandb.kvstore.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DirManager {

    private static final String IN_PROCESS_DIR = "in-process";

    public static final String DB_STATE_SUFFIX = "db-state";
    private final Path baseDir;
    public DirManager(Path baseDir){
        this.baseDir = baseDir;
    }

    public void init(){
        File inProcess = inProcessDir().toFile();
        if(!inProcess.exists()){
            inProcess.mkdirs();
        }
    }

    private Path inProcessDir(){
        return this.baseDir.resolve(IN_PROCESS_DIR);
    }

    public Path newDbStateFile() {
        return Path.of(this.baseDir + File.separator + System.currentTimeMillis() + "-" + DB_STATE_SUFFIX);
    }

    public List<Path> findStateFiles() throws IOException {
        List<Path> stateFiles = Files.find(this.baseDir,
                1,
                (path, basicFileAttributes) -> path.toFile().getName().endsWith(DB_STATE_SUFFIX)
        ).collect(Collectors.toList());
        return stateFiles;
    }

    public Path newSegmentDir() {
        return Path.of(this.baseDir + File.separator + System.currentTimeMillis() + "-" + UUID.randomUUID() + "-segment");

    }

    public Path contentFor(Path segmentDir) {
        return segmentDir.resolve("content");
    }

    public Path metadataFor(Path segmentDir) {
        return segmentDir.resolve("metadata");
    }

    public Path newInMemoryLogFile() {
        return baseDir.resolve(System.currentTimeMillis() + "-" + UUID.randomUUID() + "-log-file");
    }

    public String resolve(String f) {
        return baseDir.resolve(f).toString();
    }
}
