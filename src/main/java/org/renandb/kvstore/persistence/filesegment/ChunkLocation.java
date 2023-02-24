package org.renandb.kvstore.persistence.filesegment;

import java.io.Serializable;

public class ChunkLocation implements Serializable, Comparable<ChunkLocation>{

    private static final long serialVersionUID = 85824334L;

    private String startKey;
    private int position;
    private int size;

    public ChunkLocation(String firstKey, int position, int size) {
        this.startKey = firstKey;
        this.position = position;
        this.size = size;
    }

    public long getPosition() {
        return position;
    }

    public String getStartKey() {
        return startKey;
    }

    public int getSize() {
        return size;
    }

    @Override
    public int compareTo(ChunkLocation o) {
        return this.startKey.compareTo(o.getStartKey());
    }
}
