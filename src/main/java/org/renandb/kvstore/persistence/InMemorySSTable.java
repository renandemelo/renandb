package org.renandb.kvstore.persistence;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import org.renandb.kvstore.persistence.record.Record;
import org.renandb.kvstore.persistence.maintenance.Serializer;

/**
 * SSTable based on memory but with append-only log to guarantee durability.
 */
public class InMemorySSTable implements SSTableSegment {

    private final Path storageDir;
    private long MAX_VALUE_BYTES = 1024 * 1024 * 10; // 10MB
    private TreeMap<String, Record> records = new TreeMap<>();
    private BloomFilter bloomFilter = new BloomFilter();
    private long sizeInBytes;
    private Path logFile;

    public InMemorySSTable(Path storageDir) {
        this.storageDir = storageDir;
    }
    public synchronized InMemorySSTable init(Optional<Path> existingLogFile) throws IOException {
        if (existingLogFile.isPresent()){
            logFile = existingLogFile.get();
            restoreFromFile(logFile);
        }else{
            logFile = Path.of(storageDir + File.separator + System.currentTimeMillis() + "-" + UUID.randomUUID() + "-log-file");
            logFile.toFile().createNewFile();
        }
        return this;
    }

    private void restoreFromFile(Path path) throws IOException {
        try (InputStream is = new ByteArrayInputStream(Files.readAllBytes(path))) {
            while(true){
                ObjectInputStream ois = new ObjectInputStream(is);
                Record record = (Record) ois.readObject();
                ois.close();
                this.register(record);
            }
        } catch (java.io.EOFException ex){
          // Success! we read the whole file.
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
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
        Files.write(logFile, Serializer.serialize(record), StandardOpenOption.APPEND);
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
