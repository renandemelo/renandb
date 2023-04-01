package org.renandb.kvstore.persistence.memory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.TreeMap;

import org.renandb.kvstore.persistence.BloomFilter;
import org.renandb.kvstore.persistence.DirManager;
import org.renandb.kvstore.persistence.SSTableSegment;
import org.renandb.kvstore.persistence.Serializer;
import org.renandb.kvstore.persistence.record.Record;

/**
 * SSTable based on memory but with append-only log to guarantee durability.
 */
public class InMemorySSTable implements SSTableSegment {

    private final DirManager dirManager;
    private long MAX_VALUE_BYTES = 1024 * 1024 * 10; // 10MB
    private TreeMap<String, Record> records = new TreeMap<>();
    private BloomFilter bloomFilter = new BloomFilter();
    private long sizeInBytes;
    private Path logFile;

    public InMemorySSTable(DirManager dirManager) {
        this.dirManager = dirManager;
    }
    public synchronized InMemorySSTable init(Optional<Path> existingLogFile) throws IOException {
        if (existingLogFile.isPresent()){
            logFile = existingLogFile.get();
            restoreFromFile(logFile);
        }else{
            logFile = dirManager.newInMemoryLogFile();
            logFile.toFile().createNewFile();
        }
        return this;
    }


    private void restoreFromFile(Path path) throws IOException {
        try (InputStream is = new ByteArrayInputStream(Files.readAllBytes(path))) {
            while(true){
                int size = is.read();
                if(size == -1){
                    break;
                }
                byte[] contentBytes = new byte[size];
                is.read(contentBytes);
                Record record = Serializer.restore(contentBytes, Record.class);
                this.register(record);
            }
        }
    }

    @Override
    public synchronized Optional<Record> retrieve(String key) {
        return Optional.ofNullable(records.get(key));
    }

    @Override
    public synchronized boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    @Override
    public Path getFile() {
        return logFile;
    }

    private synchronized void register(Record record){
        this.bloomFilter.register(record.getKey());
        records.put(record.getKey(), record);
        sizeInBytes += record.sizeInBytes();
    }
    public synchronized void store(Record record) throws IOException {
        byte[] content = Serializer.serialize(record);
        try (FileOutputStream fos = new FileOutputStream(logFile.toFile(), true)){
            fos.write(content.length);
            fos.write(content);
        }
        register(record);
    }

    public boolean full() {
        return sizeInBytes > MAX_VALUE_BYTES;
    }

    public TreeMap<String, Record> getRecords() {
        return records;
    }

    public BloomFilter getBloomFilter() {
        return this.bloomFilter;
    }

    public InMemorySSTable init() throws IOException {
        return this.init(Optional.empty());
    }

}
