package org.renandb.kvstore.persistence.filesegment;

import java.util.Objects;

public class ChunkLocation implements Comparable<ChunkLocation>{

    private String startKey;
    private int position;
    private int size;

    private ChunkLocation(){

    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkLocation that = (ChunkLocation) o;
        return position == that.position && size == that.size && startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, position, size);
    }
}
