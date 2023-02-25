package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.filesegment.ChunkLocation;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.record.Record;
import org.renandb.kvstore.persistence.record.RecordChunk;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FileBasedSSTableReader {

    private static final int MAX_CHUNK_READS = 50;
    private final FileBasedSSTable table;
    private final Iterator<ChunkLocation> chunkLocationIterator;
    private Iterator<RecordChunk> chunkIterator;
    private Iterator<Record> recordIterator;
    private RecordChunk currentChunk;
    private Record nextRecord;
    private FileInputStream fis;

    public FileBasedSSTableReader(FileBasedSSTable table) {
        this.table = table;
        this.chunkLocationIterator = table.getMetadata().getChunkLocations().iterator();
        this.chunkIterator = Collections.emptyIterator();
        this.recordIterator = Collections.emptyIterator();
    }

    public FileBasedSSTableReader init() throws IOException {
        fis = new FileInputStream(table.getContentFile().toFile());
        return this;
    }

    public void readNextChunks() throws IOException {
        List<RecordChunk> recordChunkList = new ArrayList<>(MAX_CHUNK_READS);
        int counter = 0;
        while(chunkLocationIterator.hasNext() && counter <= MAX_CHUNK_READS){
            ChunkLocation location = chunkLocationIterator.next();
            RecordChunk chunk = table.readRecordChunk(location, fis);
            recordChunkList.add(chunk);
            counter++;
        }
        chunkIterator = recordChunkList.iterator();
    }

    private boolean hasNextChunk() throws IOException {
        if(!chunkIterator.hasNext()){
            readNextChunks();
        }
        return chunkIterator.hasNext();
    }

    public void close() throws IOException {
        fis.close();
    }

    public boolean hasNext() throws IOException {
        return nextRecord != null || recordIterator.hasNext() || hasNextChunk();
    }

    public void next() throws IOException {
        if(hasNext()){
           if(recordIterator.hasNext()){
               nextRecord = recordIterator.next();
           } else if(hasNextChunk()){
               currentChunk = chunkIterator.next();
               recordIterator = currentChunk.entries();
               nextRecord = recordIterator.next();
           } else{
               nextRecord = null;
           }
        } else{
            nextRecord = null;
        }
    }

    public Record offer() throws IOException {
        if(hasNext()) {
            if(nextRecord == null)
                next();
            return nextRecord;
        }
        return null;
    }
}
