package org.renandb.kvstore.integration;

import org.renandb.kvstore.persistence.LSMPersistentStorage;
import org.renandb.kvstore.util.FileUtil;
import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.util.TestingStandards;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.Assert.*;

public class LSMPersistentStorageTest {

    private LSMPersistentStorage store;
    private Path storageDir;

    @Before
    public void prepareStorage() throws IOException, ClassNotFoundException {
        storageDir = FileUtil.createTempDir();
        store = new LSMPersistentStorage(TestingStandards.getDefaultConfig().setDir(storageDir.toString())).init();
    }

    @After
    public void deleteStorage() throws IOException {
        FileUtil.deleteDir(storageDir);
    }
    @Test
    public void aKeyStoredShouldBeAbleToBeRetrieved() throws IOException {
        store.store(new KVPair("A", "1"));
        Optional<KVPair> retrievedPair = store.retrieve("A");
        assertEquals(new KVPair("A", "1"), retrievedPair.get());
    }

    @Test
    public void aKeyStoredShouldBeAbleToBeRetrievedAfterFlush() throws IOException {
        store.store(new KVPair("A", "1"));
        store.flush();
        Optional<KVPair> retrievedPair = store.retrieve("A");
        assertEquals(new KVPair("A", "1"), retrievedPair.get());
    }

    @Test
    public void aKeyRemovedDoesNotExistAnymoreAfterFlush() throws IOException {
        store.store(new KVPair("A", "1"));
        store.flush();
        store.delete("A");
        Optional<KVPair> retrievedPair = store.retrieve("A");
        assertTrue(retrievedPair.isEmpty());
    }

    @Test
    public void aKeyRemovedDoesNotExistAnymore() throws IOException {
        store.store(new KVPair("A", "1"));
        store.flush();
        store.delete("A");
        Optional<KVPair> retrievedPair = store.retrieve("A");
        assertTrue(retrievedPair.isEmpty());
    }
}
