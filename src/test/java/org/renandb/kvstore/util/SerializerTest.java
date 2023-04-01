package org.renandb.kvstore.util;

import org.junit.Test;
import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.persistence.BloomFilter;
import org.renandb.kvstore.persistence.filebased.ChunkLocation;
import org.renandb.kvstore.persistence.filebased.SSTableMetadata;
import org.renandb.kvstore.persistence.state.DbState;
import org.renandb.kvstore.persistence.Serializer;

import org.renandb.kvstore.persistence.record.Record;
import org.renandb.kvstore.persistence.record.RecordChunk;
import org.renandb.kvstore.persistence.state.SegmentReference;
import org.renandb.kvstore.persistence.state.SegmentType;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class SerializerTest {

    @Test
    public void savedAndRecordedRecordAreTheSame() throws IOException {
        KVPair expected = new KVPair("AAA", "BBB");
        byte[] bytes = Serializer.serialize(Record.from(expected));
        Record record = Serializer.restore(bytes, Record.class);
        assertEquals(expected, record.toPair());
    }

    @Test
    public void canSaveRecordChunk() throws IOException {
        RecordChunk chunk = new RecordChunk();
        chunk.add(Record.from(new KVPair("CCC","DDD")));
        chunk.add(Record.from(new KVPair("AAA","BBB")));
        chunk.add(Record.newEmpty("EEE"));
        byte[] bytes = Serializer.serialize(chunk);
        RecordChunk record = Serializer.restore(bytes, RecordChunk.class);
        ArrayList<Map.Entry<String, Record>> entrySet = new ArrayList<>(record.getRecords().entrySet());
        assertEquals("AAA", entrySet.get(0).getKey());
        assertEquals(new KVPair("AAA", "BBB"), entrySet.get(0).getValue().toPair());
        assertEquals("CCC", entrySet.get(1).getKey());
        assertEquals(new KVPair("CCC", "DDD"), entrySet.get(1).getValue().toPair());
        assertEquals("EEE", entrySet.get(2).getKey());
        assertTrue(entrySet.get(2).getValue().isEmpty());
    }


    @Test
    public void canSaveSSTableMetadata() throws IOException {
        BloomFilter bloomFilter = new BloomFilter();
        bloomFilter.register("AAA").register("BBB").register("CCC");

        List<ChunkLocation> locations = List.of(new ChunkLocation("A", 0, 123),
                new ChunkLocation("B", 123, 200),
                new ChunkLocation("C", 323, 100));

        SSTableMetadata ssTableMetadata = new SSTableMetadata(bloomFilter);
        ssTableMetadata.add(new ChunkLocation("A", 0, 123));
        ssTableMetadata.add(new ChunkLocation("B", 123, 200));
        ssTableMetadata.add(new ChunkLocation("C", 323, 100));

        byte[] bytes = Serializer.serialize(ssTableMetadata);

        SSTableMetadata metadata = Serializer.restore(bytes, SSTableMetadata.class);
        assertArrayEquals(bloomFilter.getFilters(), metadata.getBloomFilter().getFilters());

        ArrayList<ChunkLocation> actualLocations = new ArrayList<>(ssTableMetadata.getChunkLocations());
        assertEquals(locations.get(0), actualLocations.get(0));
        assertEquals(locations.get(1), actualLocations.get(1));
        assertEquals(locations.get(2), actualLocations.get(2));

    }

    @Test
    public void savedDbStateCanBeRestored() throws IOException {
        DbState expected = new DbState();
        List<SegmentReference> expectedSegments = List.of(
                new SegmentReference(SegmentType.IN_MEMORY, "123-log-file"),
                new SegmentReference(SegmentType.FILE_BASED, "333-segment-dir")
                );
        expected.setSegmentReferenceList(expectedSegments);
        byte[] bytes = Serializer.serialize(expected);
        DbState actual = Serializer.restore(bytes, DbState.class);

        List<SegmentReference> actualSegments = actual.getSegmentReferenceList();
        assertEquals(expectedSegments.get(0).getType(), actualSegments.get(0).getType());
        assertEquals(expectedSegments.get(0).getBasePath(), actualSegments.get(0).getBasePath());
        assertEquals(expectedSegments.get(1).getType(), actualSegments.get(1).getType());
        assertEquals(expectedSegments.get(1).getBasePath(), actualSegments.get(1).getBasePath());
    }



}
