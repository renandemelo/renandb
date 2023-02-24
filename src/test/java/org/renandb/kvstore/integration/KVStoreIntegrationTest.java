package org.renandb.kvstore.integration;

import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.KVStore;
import org.renandb.kvstore.util.FileUtil;
import org.renandb.kvstore.util.KVUtil;
import org.renandb.kvstore.util.TestingStandards;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class KVStoreIntegrationTest {

    private static final long MAX_ACCEPTABLE_RESPONSE_TIME_MS = 50;
    private static int HUNDRED_THOUSAND = 100 * 1000;
    private static Logger LOGGER =
            java.util.logging.Logger.getLogger(KVStoreIntegrationTest.class.getName());
    private KVStore store;
    private Path storageDir;

    @Before
    public void prepareStorage() throws IOException {
        storageDir = FileUtil.createTempDir();
        store = new KVStore(TestingStandards.getDefaultConfig().setCacheSize(20).setDir(storageDir.toString()));
    }
    @After
    public void deleteStorage() throws IOException {
        FileUtil.deleteDir(storageDir);
    }
    @Test
    public void keyAccessIsFastAndStableEvenInHundredsOfThousandsOfRecords() throws IOException {

        putAll(KVUtil.generateRandomPairs(HUNDRED_THOUSAND));
        store.put(new KVPair("someKey", "ABC"));

        long beforeFirstAccessTime = System.currentTimeMillis();
        assertEquals(new KVPair("someKey", "ABC"), store.get("someKey").get());
        // recently used key is in the cache, it should be very fast...
        long firstAccessExecutionTime = System.currentTimeMillis() - beforeFirstAccessTime;

        // Delete key afterwards in the middle of hundreds of thousands of records...
        store.delete("someKey");
        assertTrue(store.get("someKey").isEmpty());

        putAll(KVUtil.generateRandomPairs(HUNDRED_THOUSAND));
        long beforeSecondAccessTime = System.currentTimeMillis();
        assertTrue(store.get("someKey").isEmpty());
        long secondAccessExecutionTime = System.currentTimeMillis() - beforeSecondAccessTime;

        store.put(new KVPair("someKey", "anotherValue"));
        putAll(KVUtil.generateRandomPairs(HUNDRED_THOUSAND));

        long beforeThirdAccessTime = System.currentTimeMillis();
        assertEquals(new KVPair("someKey", "anotherValue"), store.get("someKey").get());
        long thirdAccessExecutionTime = System.currentTimeMillis() - beforeThirdAccessTime;


        LOGGER.info("First access execution time (ms):" + firstAccessExecutionTime);
        LOGGER.info("Second access execution time (ms):" + secondAccessExecutionTime);
        LOGGER.info("Third access execution time (ms):" + thirdAccessExecutionTime);

        assertMaxAcceptableTime(firstAccessExecutionTime, "First access");
        assertMaxAcceptableTime(secondAccessExecutionTime, "Second access");
        assertMaxAcceptableTime(thirdAccessExecutionTime, "Third access");
    }

    private void assertMaxAcceptableTime(long firstAccessExecutionTime, String accessDescription) {
        assertTrue(accessDescription + firstAccessExecutionTime +" should be faster than " + MAX_ACCEPTABLE_RESPONSE_TIME_MS, firstAccessExecutionTime  < MAX_ACCEPTABLE_RESPONSE_TIME_MS);
    }

    private void putAll(List<KVPair> pairs) throws IOException {
        for(KVPair pair : pairs){
            store.put(pair);
        }
    }

}
