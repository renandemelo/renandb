package org.renandb.kvstore.integration;

import org.renandb.kvstore.persistence.LSMPersistentStorage;
import org.renandb.kvstore.util.FileUtil;
import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.util.KVUtil;
import org.renandb.kvstore.util.TestingStandards;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MillionsOfRecordsStorageIntegrationTest {

    private static final long MAX_ACCEPTABLE_RESPONSE_TIME_MS = 500;
//    public static final int WAIT_TO_OBSERVE_BEHAVIOR_MS = 60000;
    public static final int WAIT_TO_OBSERVE_BEHAVIOR_MS = 0;
    private static Logger LOGGER =
            java.util.logging.Logger.getLogger(MillionsOfRecordsStorageIntegrationTest.class.getName());
    private static final int ONE_MILLION = 1000 * 1000;
    private LSMPersistentStorage store;
    private Path storageDir;

    @Before
    public void prepareStorage() throws IOException, ClassNotFoundException {
        storageDir = FileUtil.createTempDir();
        storageDir = Path.of("/tmp/mydb");
        LOGGER.info("Using folder:" + storageDir);
        store = new LSMPersistentStorage(TestingStandards.getDefaultConfig().setDir(storageDir.toString())).init();
    }
    @After
    public void deleteStorage() throws IOException {
//        FileUtil.deleteDir(storageDir);
    }
    @Test
    public void keyAccessIsFastEvenInMillionsOfRecords() throws IOException {
        storeAll(KVUtil.generateRandomPairs(ONE_MILLION));
        // Store pair in the middle of two million records
        store.store(new KVPair("someKey", "ABC"));
        store.store(new KVPair("anotherKey", "DEF"));
        storeAll(KVUtil.generateRandomPairs(ONE_MILLION));

        long beforeFirstAccessTime = System.currentTimeMillis();
        Assert.assertEquals(new KVPair("someKey", "ABC"), store.retrieve("someKey").get());
        long firstAccessExecutionTime = System.currentTimeMillis() - beforeFirstAccessTime;

        // Delete key afterwards in the middle of 3 million records...
        store.delete("someKey");
        List<KVPair> pairs = KVUtil.generateRandomPairs(ONE_MILLION);
        storeAll(pairs);
        long beforeSecondAccessTime = System.currentTimeMillis();
        assertTrue(store.retrieve("someKey").isEmpty());
        long secondAccessExecutionTime = System.currentTimeMillis() - beforeSecondAccessTime;

        LOGGER.info("First access execution time (ms):" + firstAccessExecutionTime);
        LOGGER.info("Second access execution time (ms):" + secondAccessExecutionTime);


        assertMaxAcceptableTime(firstAccessExecutionTime, "First access");
        assertMaxAcceptableTime(secondAccessExecutionTime, "Second access");

        waitToObserveAsyncBehavior();

        KVPair targetPair = pairs.get(ONE_MILLION / 2); // middle of first million
        long beforeThirdAccessTime = System.currentTimeMillis();
        Assert.assertEquals(targetPair, store.retrieve(targetPair.getKey()).get());
        LOGGER.info("Third access (after compaction) (ms):" + (System.currentTimeMillis() - beforeThirdAccessTime));

    }

    private void waitToObserveAsyncBehavior() {
        LOGGER.info("Waiting " + WAIT_TO_OBSERVE_BEHAVIOR_MS + "ms to observe async behavior.");
        try {
            Thread.sleep(WAIT_TO_OBSERVE_BEHAVIOR_MS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeAll(List<KVPair> pairs) throws IOException {
        long beforeStoreAll = System.currentTimeMillis();
        for(KVPair pair : pairs){
            store.store(pair);
        }
        long totalTime = System.currentTimeMillis() - beforeStoreAll;
        LOGGER.info("Stored " + (pairs.size()/ONE_MILLION) + " million elements in " + totalTime + "ms - avg duration per item: " + (totalTime/pairs.size() + "ms")  );
    }

    private void assertMaxAcceptableTime(long firstAccessExecutionTime, String accessDescription) {
        assertTrue(accessDescription + " " + firstAccessExecutionTime +" should be faster than " + MAX_ACCEPTABLE_RESPONSE_TIME_MS, firstAccessExecutionTime  < MAX_ACCEPTABLE_RESPONSE_TIME_MS);
    }

}
