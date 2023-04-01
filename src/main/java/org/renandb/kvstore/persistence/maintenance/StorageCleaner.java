package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.DirManager;
import org.renandb.kvstore.persistence.state.DbState;
import org.renandb.kvstore.persistence.state.SegmentReference;
import org.renandb.kvstore.persistence.state.StateManager;
import org.renandb.kvstore.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageCleaner implements Runnable{

    private static final int MAX_WAIT_TIME_MILLIS = 40 * 1000; // 50 seconds
    private final DirManager dirManager;
    private final LoadChecker loadChecker;
    private final StateManager stateManager;
    private boolean dirty = false;

    public StorageCleaner(DirManager dirManager, LoadChecker usageChecker, StateManager stateManager){
        this.dirManager = dirManager;
        this.loadChecker = usageChecker;
        this.stateManager = stateManager;
    }

    public synchronized void notifyChanges() {
        setDirty(true);
        this.notifyAll();
    }

    public void init() {
        new Thread(this).start();
        notifyChanges();
    }

    @Override
    public void run() {
        while(true){
            waitUntilDirty();
            try {
                cleanOutdatedFiles();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            setDirty(false);
            try {
                Thread.sleep(loadChecker.getWaitTimeForCurrentLoad(MAX_WAIT_TIME_MILLIS));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    private void cleanOutdatedFiles() throws IOException {
        List<DbState> stateHistory = stateManager.getStateHistory();
        if(!stateHistory.isEmpty()){
            List<String> filesInCurrentState = stateHistory.get(0).getSegmentReferenceList().stream()
                    .map(r -> r.getBasePath()).collect(Collectors.toList());
            Stream<Path> relatedFilesForDelete = stateHistory.stream()
                        .map(DbState::getSegmentReferenceList)
                        .flatMap(List::stream)
                        .map(SegmentReference::getBasePath)
                        .filter(f -> !filesInCurrentState.contains(f))
                        .map(f -> Path.of(dirManager.resolve(f)));
            List<Path> filesToDelete = Stream.concat(relatedFilesForDelete,
                            stateHistory.stream().skip(1)
                    .map(DbState::getFile)).collect(Collectors.toList());

            for(Path toDelete : filesToDelete){
                File f = toDelete.toFile();
                if(f.isDirectory()) FileUtil.deleteDir(toDelete);
                else if (f.exists()) f.delete();
            }
        }
    }

    private void waitUntilDirty() {
        synchronized (this){
            while (!dirty){
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
