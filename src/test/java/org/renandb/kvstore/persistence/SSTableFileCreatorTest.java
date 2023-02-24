package org.renandb.kvstore.persistence;

import org.renandb.kvstore.util.FileUtil;
import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.filesegment.SSTableFileCreator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.renandb.kvstore.persistence.record.Record;

public class SSTableFileCreatorTest {

    private static Logger LOGGER =
            java.util.logging.Logger.getLogger(SSTableFileCreatorTest.class.getName());
    private Path storageDir;

    @Before
    public void prepareStorage() throws IOException {
        storageDir = Files.createTempDirectory("kvstore");
        LOGGER.info("Storage at: " + storageDir);
    }

    @After
    public void deleteStorage() throws IOException {
        FileUtil.deleteDir(storageDir);
    }
    @Test
    public void listOfRecordsShouldBeStoredAndThenRetrieved() throws IOException {
        InMemorySSTable inMemorySSTable = new InMemorySSTable(this.storageDir).init();
        List<KVPair> pairs = generatePairs(1000);
        for(KVPair pair: pairs){
            inMemorySSTable.store(Record.from(pair));
        }

        FileBasedSSTable ssTable = new SSTableFileCreator(inMemorySSTable, storageDir, 100).createFullNew();
        for(KVPair pair : pairs){
            String key = pair.getKey();
            assertTrue(ssTable.mightContain(key));
            Optional<Record> actual = ssTable.retrieve(key);
            if(actual.isEmpty()){
                ssTable.retrieve(key);
            }
            Assert.assertEquals(pair, actual.get().toPair());
        }
    }

    @Test
    public void ssTableShouldNotRetrieveUnsavedKeys() throws IOException {
        InMemorySSTable inMemorySSTable = new InMemorySSTable(this.storageDir).init();
        for(KVPair pair: generatePairs(1000)){
            inMemorySSTable.store(Record.from(pair));
        }

        FileBasedSSTable ssTable = new SSTableFileCreator(inMemorySSTable, storageDir, 100).createFullNew();

        assertTrue(ssTable.retrieve("AnotherKey").isEmpty());
    }

    private List<KVPair> generatePairs(int size) {
        List<KVPair> pairs = new ArrayList<>(size);
        String prefix = UUID.randomUUID().toString();
        for(int i = 0; i < size; i++){
            pairs.add(new KVPair(prefix + "-" + i, "" + i));
        }
        return pairs;
    }

}
