package org.renandb.kvstore.persistence.record;

import java.io.Serializable;
import java.util.*;

public class RecordChunk implements Serializable {

    public static int BLOCK_SIZE_IN_BYTES = 1024 * 4; // 4KB
    private TreeMap<String, Record> records = new TreeMap<>();
    private int sizeInBytes = 0;

    public void add(Record record) {
        records.put(record.getKey(), record);
        sizeInBytes += record.sizeInBytes();
    }

    public boolean full(int maxSizeInBytes) {
        return sizeInBytes >= maxSizeInBytes;
    }

    public boolean empty() {
        return sizeInBytes == 0;
    }

    public Optional<Record> get(String key) {
        Record record = records.get(key);
        return record != null? Optional.of(record) : Optional.empty();
    }

    public Iterator<Record> entries() {
        return records.values().iterator();
    }

    public String firstKey() {
        return !empty()? records.keySet().iterator().next() : null;
    }


    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public void setSizeInBytes(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public TreeMap<String, Record> getRecords() {
        return records;
    }

    public void setRecords(TreeMap<String, Record> records) {
        this.records = records;
    }

}
