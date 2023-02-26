package org.renandb.kvstore.persistence;

import org.renandb.kvstore.KVPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.renandb.kvstore.config.StorageConfig;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.filesegment.SSTableFileCreator;
import org.renandb.kvstore.persistence.maintenance.MergedSegmentReceiver;
import org.renandb.kvstore.persistence.maintenance.SegmentMerger;
import org.renandb.kvstore.persistence.maintenance.StateManager;
import org.renandb.kvstore.persistence.maintenance.LoadChecker;
import org.renandb.kvstore.persistence.record.Record;

/**
 * This is an implementation of Log-structured-merge tree (LSM) storage
 * used by databases like Cassandra and leveldb. This one does not have
 * (an asynchronous) compaction mechanism neither a restore mechanism (in case of failovers) but all
 * data is stored in the disk at all times.
 * It offers:
 * - extremely high write throughput
 * - extremely fast recent reads
 * - empirically fast random reads (powered by segments skipped using Bloom Filters).
 *
 * Details: https://en.wikipedia.org/wiki/Log-structured_merge-tree
 *
 * TODO: Async compaction mechanism and restoring from crashes - both possible with files saved here.
 */
public class LSMPersistentStorage implements MergedSegmentReceiver {

    private final StateManager stateManager;
    private final StorageConfig config;
    private final DirManager dirManager;
    private LinkedList<SSTableSegment> segments;
    private final LoadChecker loadChecker;
    private SegmentMerger segmentMerger;

    public LSMPersistentStorage(StorageConfig config){
        this.config = config;
        this.loadChecker = new LoadChecker();
        this.dirManager = new DirManager(config.getDir());
        this.stateManager = new StateManager(dirManager, loadChecker );
        this.segmentMerger = new SegmentMerger(dirManager, loadChecker, this);
    }

    public synchronized LSMPersistentStorage init() throws IOException, ClassNotFoundException {
        dirManager.init();
        if(config.isSyncProcessesActive()){
            this.stateManager.init();
            this.segmentMerger.init();
        }
        segments = new LinkedList<>(this.stateManager.getSegmentList());
        if(segments.isEmpty()){
            segments.add(new InMemorySSTable(this.dirManager).init());
        }
        this.segmentMerger.notifyChange(segments);
        return this;
    }

    private Optional<Record> findMostRecentFor(final String key) throws IOException {
        List<SSTableSegment> dataSources;
        synchronized (this){
            dataSources = new ArrayList<>(segments);
        }
        for(SSTableSegment source : dataSources){
            if(source.mightContain(key)){
                Optional<Record> pair = source.retrieve(key);
                if(pair.isPresent()) return pair;
            }
        }
        return Optional.empty();
    }

    public Optional<KVPair> retrieve(String key) throws IOException {
        loadChecker.notifyUsage();
        Optional<Record> record = findMostRecentFor(key);
        if(record.isEmpty() || record.get().isEmpty()) return Optional.empty();
        return Optional.of(record.get().toPair());
    }
    public void update(Record record) throws IOException {
//        loadChecker.notifyUsage();
        InMemorySSTable segmentToStore;
        synchronized (this) {
            segmentToStore = (InMemorySSTable) segments.get(0);
        }
        segmentToStore.store(record);
        if (segmentToStore.full()){
            flush();
        }
    }

    /**
     * This closes the in-memory first segment, creates an empty one fast
     * (so other threads can continue reading from a "final" object) and replaces to a file-based
     * version after it is ready.
     */
    public void flush(){
        try{
            InMemorySSTable segmentToClose = null;
            synchronized (this){
                segmentToClose = (InMemorySSTable) segments.get(0);
                synchronized (segmentToClose){
                    // Replace in-memory for another one fast!
                    // Don't stop serving read requests just because we're closing the in-memory segment.
                    this.segments.addFirst(new InMemorySSTable(this.dirManager).init());
                }
                saveNewDbState();
            }
            // Producing an equivalent file-based segment can take some time... not blocking in the meanwhile :)
            FileBasedSSTable fileBasedSegment = new SSTableFileCreator(segmentToClose, this.dirManager).createFullNew();
            synchronized (this){
                int indexToReplace = this.segments.indexOf(segmentToClose); // should be first.
                this.segments.set(indexToReplace, fileBasedSegment);
                saveNewDbState();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private synchronized void saveNewDbState() throws IOException {
        this.stateManager.saveState(segments);
        this.segmentMerger.notifyChange(new ArrayList<>(this.segments));
    }

    public void store(KVPair pair) throws IOException {
        loadChecker.notifyUsage();
        update(Record.from(pair));
    }

    public void delete(String key) throws IOException {
        loadChecker.notifyUsage();
        update(Record.newEmpty(key));
    }

    @Override
    public synchronized boolean onMergedSegmentAvailable(FileBasedSSTable merged, FileBasedSSTable first, FileBasedSSTable second) throws IOException {
        int indexFirst = segments.indexOf(first);
        int indexSecond = segments.indexOf(second);
        if(indexFirst >= 0 && indexSecond >= 0){
            segments.set(indexFirst, merged);
            segments.remove(indexSecond);
            saveNewDbState();
            return true;
        }
        return false;
    }
}
