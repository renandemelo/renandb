package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.DirManager;
import org.renandb.kvstore.persistence.InMemorySSTable;
import org.renandb.kvstore.persistence.SSTableSegment;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.state.SegmentReference;
import org.renandb.kvstore.persistence.state.SegmentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class StateManager {
    private final DirManager dirManager;

    private LinkedList<SSTableSegment> segmentList = new LinkedList<>();
    private StorageCleaner storageCleaner;

    public StateManager(DirManager dirManager, LoadChecker loadChecker){
        this.dirManager = dirManager;
        this.storageCleaner = new StorageCleaner(dirManager, loadChecker, this);
    }

    public void init() throws IOException, ClassNotFoundException {
        Optional<Path> latestDBState = findLatestDBState();
        if(!latestDBState.isEmpty()){
            DbState state = getDbState(latestDBState.get());
            for(SegmentReference segmentReference : state.getSegmentReferenceList()){
                Path basePath = Path.of(segmentReference.getBasePath());
                if(segmentReference.getType() == SegmentType.FILE_BASED){
                    segmentList.add(new FileBasedSSTable(basePath, this.dirManager).init());
                }else{
                    segmentList.add(new InMemorySSTable(dirManager).init(Optional.of(basePath)));
                }
            }
        }
        storageCleaner.init();
    }

    private Optional<Path> findLatestDBState() throws IOException {
        List<Path> stateFileHistory = getStateFileHistory();
        if(stateFileHistory.isEmpty()) return Optional.empty();
        return Optional.of(stateFileHistory.get(0));
    }

    private List<Path> getStateFileHistory() throws IOException {
        List<Path> stateFileHistory = new StateFileFinder(this.dirManager).findStateFileHistory();
        return stateFileHistory;
    }

    public List<DbState> getStateHistory() throws IOException {
        List<Path> files = this.getStateFileHistory();
        return files.stream().map(p -> {
            try {
                DbState dbState = getDbState(p);
                return dbState;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    private static DbState getDbState(Path p) throws IOException {
        DbState dbState = Serializer.restore(Files.readAllBytes(p), DbState.class);
        dbState.setFile(p);
        return dbState;
    }

    public LinkedList<SSTableSegment> getSegmentList() {
        return segmentList;
    }

    public void saveState(List<SSTableSegment> segments) throws IOException {
        Path dbStateFile = this.dirManager.newDbStateFile();
        DbState dbState = DbState.from(segments);
        Files.write(dbStateFile, Serializer.serialize(dbState));
        storageCleaner.notifyChanges();
    }
}
