package org.renandb.kvstore.persistence.record;

import java.io.Serializable;
import java.util.*;

public class RecordChunk implements Serializable {
    private static final long serialVersionUID = 8582433445454L;

    public static int BLOCK_SIZE_IN_BYTES = 1024 * 4; // 4KB
    private TreeMap<String, Record> records = new TreeMap<>();
    private int sizeInBytes = 0;

    public void add(Record record) {
        records.put(record.getKey(), record);
        sizeInBytes += record.getSizeInBytes();
    }

    public boolean isFull(int maxSizeInBytes) {
        return sizeInBytes >= maxSizeInBytes;
    }


    public boolean isEmpty() {
        return sizeInBytes == 0;
    }

    public Optional<Record> get(String key) {
        Record record = records.get(key);
        return record != null? Optional.of(record) : Optional.empty();
    }

    public Iterator<Record> getEntries() {
        return records.values().iterator();
    }

    public String getFirstKey() {
        return !isEmpty()? records.keySet().iterator().next() : null;
    }
}
