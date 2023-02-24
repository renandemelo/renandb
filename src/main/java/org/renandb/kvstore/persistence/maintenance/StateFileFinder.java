package org.renandb.kvstore.persistence.maintenance;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StateFileFinder {

    public static final String DB_STATE_SUFFIX = "db-state";

    private final Path baseDir;

    public StateFileFinder(Path baseDir){
        this.baseDir = baseDir;
    }
    public List<Path> findStateFiles(){
        File baseDirFile = baseDir.toFile();
        if(baseDirFile == null || !baseDirFile.exists()) return List.of();
        List<Path> stateFiles = Arrays.stream(baseDirFile.list())
                .filter(s -> s.endsWith(DB_STATE_SUFFIX))
                .sorted(Comparator.reverseOrder())
                .map(s -> Path.of(baseDir + File.separator + s)).collect(Collectors.toList());
        return stateFiles;
    }
}
