package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.InMemorySSTable;
import org.renandb.kvstore.persistence.SSTableSegment;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class StateManager {
    private final Path baseDir;
    private LinkedList<SSTableSegment> segmentList = new LinkedList<>();
    private StorageCleaner storageCleaner;

    public StateManager(Path baseDir, LoadChecker loadChecker){
        this.baseDir = baseDir;
        this.storageCleaner = new StorageCleaner(baseDir, loadChecker);
    }

    public void init() throws IOException, ClassNotFoundException {
        Optional<Path> latestDBState = findLatestDBState();
        if(!latestDBState.isEmpty()){
            for(String filename : Files.readAllLines(latestDBState.get())){
                Path segmentFile = Path.of(baseDir + File.separator + filename);
                if(segmentFile.toFile().isDirectory()){
                    segmentList.add(new FileBasedSSTable(segmentFile).init());
                }else{
                    segmentList.add(new InMemorySSTable(baseDir).init(Optional.of(segmentFile)));
                }
            }
        }
        storageCleaner.init();
    }

    private Optional<Path> findLatestDBState() {
        List<Path> stateFiles = new StateFileFinder(this.baseDir).findStateFiles();
        if(stateFiles.isEmpty()) return Optional.empty();
        return Optional.of(stateFiles.get(0));
    }
    public LinkedList<SSTableSegment> getSegmentList() {
        return segmentList;
    }

    public void saveState(List<String> filenames) throws IOException {
        Path dbStateFile = Path.of(baseDir + File.separator + System.currentTimeMillis() + "-" + StateFileFinder.DB_STATE_SUFFIX);
        Files.write(dbStateFile,filenames, StandardCharsets.UTF_8);
        storageCleaner.notifyChanges();
    }
}
