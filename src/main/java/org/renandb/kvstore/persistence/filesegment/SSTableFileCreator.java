package org.renandb.kvstore.persistence.filesegment;

import org.renandb.kvstore.persistence.BloomFilter;
import org.renandb.kvstore.persistence.record.RecordChunk;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.renandb.kvstore.persistence.record.Record;
import org.renandb.kvstore.persistence.InMemorySSTable;
import org.renandb.kvstore.persistence.maintenance.Serializer;

public class SSTableFileCreator {

    private final String contentFile;
//    private final TreeMap<String, Record> records;
    private final Path segmentDir;
    private final int maxChunkSizeInBytes;
    private InMemorySSTable inMemorySegment;
    private SSTableMetadata metadata;
    private RecordChunk current;

    int contentOffset = 0;
    private BufferedOutputStream bos;
    private FileOutputStream fos;

    public SSTableFileCreator(BloomFilter bloomFilter, Path baseDir, int maxChunkSizeInBytes) {
        segmentDir = Path.of(baseDir.toAbsolutePath() + File.separator + System.currentTimeMillis() + "-" + UUID.randomUUID() + "-segment");
        contentFile = segmentDir + File.separator + "content";
        metadata = new SSTableMetadata(bloomFilter);
        this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    }
    public SSTableFileCreator(BloomFilter bloomFilter, Path baseDir) {
        this(bloomFilter, baseDir, RecordChunk.BLOCK_SIZE_IN_BYTES);
    }

    public SSTableFileCreator(InMemorySSTable openSegment, Path baseDir, int maxChunkSizeInBytes) {
        this(openSegment.getBloomFilter(), baseDir, maxChunkSizeInBytes);
        this.inMemorySegment = openSegment;
    }
    public SSTableFileCreator(InMemorySSTable openSegment, Path baseDir) {
        this(openSegment,baseDir,RecordChunk.BLOCK_SIZE_IN_BYTES);
    }

    public FileBasedSSTable createFullNew() throws IOException {
        startNew();
        try{
            processRecords(inMemorySegment.getRecords().values().iterator());
            FileBasedSSTable segment = finishNew();
            return segment;
        }
        finally{
            close();
        }
    }

    public void close() throws IOException {
        bos.close();
        fos.close();
    }

    public FileBasedSSTable finishNew() throws IOException {
        FileBasedSSTable segment = new FileBasedSSTable(segmentDir);
        segment.init(metadata);
        return segment;
    }

    public void startNew() throws IOException {
        Files.createDirectories(segmentDir);
        fos = new FileOutputStream(contentFile, false);
        bos = new BufferedOutputStream(fos);
    }


    public void processRecords(Iterator<Record> iterator) throws IOException {
        openChunk();
        while(iterator.hasNext()){
            Record record = iterator.next();
            current.add(record);
            if(current.full(maxChunkSizeInBytes)){
                closeChunk();
            }
        }
        if(!current.empty()) {
            closeChunk();
        }
    }

    private void closeChunk() throws IOException {
        byte[] serialized = Serializer.serialize(current);
        int chunkSize = serialized.length;
        bos.write(serialized);
        metadata.add(new ChunkLocation(current.firstKey(), contentOffset, chunkSize));
        contentOffset += chunkSize;
        openChunk();
    }

    private void openChunk() {
        this.current = new RecordChunk();
    }

}
