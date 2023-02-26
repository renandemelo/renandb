package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.BloomFilter;
import org.renandb.kvstore.persistence.DirManager;
import org.renandb.kvstore.persistence.SSTableSegment;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.filesegment.SSTableFileCreator;
import org.renandb.kvstore.persistence.record.Record;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SegmentMerger implements Runnable{
    private static final int MAX_WAIT_TIME = 20000;

    private static final int MAX_RECORDS_READ = 50000;

    private final LoadChecker loadChecker;
    private final MergedSegmentReceiver receiver;
    private final DirManager dirManager;
    private List<SSTableSegment> ssTableSegments = new ArrayList<>();

    public SegmentMerger(DirManager dirManager, LoadChecker loadChecker, MergedSegmentReceiver receiver) {
        this.dirManager = dirManager;
        this.loadChecker = loadChecker;
        this.receiver = receiver;
    }

    public synchronized void notifyChange(List<SSTableSegment> ssTableSegments) {
        this.ssTableSegments = ssTableSegments;
        notifyAll();
    }

    public synchronized void init() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        while(true){
            FileBasedSSTable table1;
            FileBasedSSTable table2;
            synchronized (this){
                try {
                    List<SSTableSegment> fileBasedSegments = ssTableSegments.stream().filter(sc -> sc instanceof FileBasedSSTable).collect(Collectors.toList());
                    if (fileBasedSegments.size() < 2){
                        wait();
                        continue;
                    }
                    int index = selectLowestConsecutivePairIndex(fileBasedSegments);
                    table1 = (FileBasedSSTable) fileBasedSegments.get(index);
                    table2 = (FileBasedSSTable) fileBasedSegments.get(index + 1);

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            try {

                BloomFilter mergedFilter = new BloomFilter(table1.getMetadata().getBloomFilter(), table2.getMetadata().getBloomFilter());

                MergeHandler mergeHandler = new MergeHandler(table1, table2);
                mergeHandler.init();
                SSTableFileCreator fileCreator = new SSTableFileCreator(mergedFilter, this.dirManager);

                fileCreator.startNew();
                try{
                    while(mergeHandler.hasNext()) {
                        List<Record> mergedRecords = mergeHandler.getNext(MAX_RECORDS_READ);
                        fileCreator.processRecords(mergedRecords.iterator());
                        Thread.sleep(loadChecker.getWaitTimeForCurrentLoad(MAX_WAIT_TIME));
                    }
                } finally {
                    fileCreator.close();
                }
                FileBasedSSTable newSegment = fileCreator.finishNew();
                boolean received = this.receiver.onMergedSegmentAvailable(newSegment, table1, table2);
                if(!received){
                    newSegment.destroyFromDisk();
                }
                Thread.sleep(loadChecker.getWaitTimeForCurrentLoad(MAX_WAIT_TIME));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private int selectLowestConsecutivePairIndex(List<SSTableSegment> fileBasedSegments) {
        FileBasedSSTable[] array = fileBasedSegments.toArray(new FileBasedSSTable[0]);

        int minValue = Integer.MAX_VALUE;
        int minIndex = 0;
        for(int i = 0; i < (array.length - 1); i++){
            int currentSize = array[i].getMetadata().getChunkLocations().size();
            int nextSize = array[i + 1].getMetadata().getChunkLocations().size();
            int consecutiveSize = currentSize + nextSize;
            if(minValue >= consecutiveSize){
                minIndex = i;
                minValue = consecutiveSize;
            }
        }
        return minIndex;
    }

}
