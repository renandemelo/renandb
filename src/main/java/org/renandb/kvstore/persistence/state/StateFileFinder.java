package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.DirManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class StateFileFinder {

    private final DirManager dirManager;

    public StateFileFinder(DirManager dirManager){
        this.dirManager = dirManager;
    }
    public List<Path> findStateFileHistory() throws IOException {
        List<Path> files = dirManager.findStateFiles().stream()
                .sorted((c1, c2) -> c2.toFile().getName().compareTo(c1.toFile().getName()))
                .collect(Collectors.toList());
        return files;
    }
}
