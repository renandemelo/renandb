package org.renandb.kvstore.persistence.filesegment;

import org.renandb.kvstore.persistence.BloomFilter;

import java.io.Serializable;
import java.util.TreeSet;

public class SSTableMetadata implements Serializable {

    private static final long serialVersionUID = 8582433437601788991L;
    private BloomFilter bloomFilter;
    private TreeSet<ChunkLocation> chunkLocations = new TreeSet<>();
    public SSTableMetadata(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public void add(ChunkLocation chunkLocation) {
        chunkLocations.add(chunkLocation);
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public ChunkLocation findCandidateFor(String key) {
        // Finds floor - latest element where (chunk.startKey <= key)
        return this.chunkLocations.floor(new ChunkLocation(key, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    public TreeSet<ChunkLocation> getChunkLocations() {
        return this.chunkLocations;
    }
}
