package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageCleaner implements Runnable{

    private static final int MAX_WAIT_TIME_MILLIS = 40 * 1000; // 50 seconds
    private final Path baseDir;
    private final LoadChecker loadChecker;
    private boolean dirty = false;

    public StorageCleaner(Path baseDir, LoadChecker usageChecker){
        this.baseDir = baseDir;
        this.loadChecker = usageChecker;
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
        List<Path> stateFiles = new StateFileFinder(this.baseDir).findStateFiles();
        List<List<String>> linesForState = stateFiles.stream().map(p -> {
            try {
                return Files.readAllLines(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        if(!linesForState.isEmpty()){
            Set<String> currentRelatedFiles = new HashSet<>(linesForState.get(0));
            Stream<Path> relatedFilesForDelete = linesForState.stream()
                        .flatMap(List::stream).filter(f -> !currentRelatedFiles.contains(f))
                        .map(f -> Path.of(baseDir + File.separator + f));
            List<Path> filesToDelete = Stream.concat(relatedFilesForDelete, stateFiles.stream().skip(1)).collect(Collectors.toList());

            for(Path toDelete : filesToDelete){
                File f = toDelete.toFile();
                if(f.isDirectory()) FileUtil.deleteDir(toDelete);
                else if (f.exists()) f.delete();
            }
            filesToDelete.forEach(f -> {

            });
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
