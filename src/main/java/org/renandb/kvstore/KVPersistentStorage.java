package org.renandb.kvstore;

import org.renandb.kvstore.config.StorageConfig;
import org.renandb.kvstore.persistence.LSMPersistentStorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class KVPersistentStorage {

    private LSMPersistentStorage internalStorage;

    Optional<KVPair> retrieve(String key) {
        try {
            return internalStorage.retrieve(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    void store(KVPair kvPair) {
        try {
            internalStorage.store(kvPair);
        } catch (IOException e) {
            throw new RuntimeException("Error storing keyPair:" + kvPair,e);
        }
    }

    void delete(String key) {
        try {
            internalStorage.delete(key);
        } catch (IOException e) {
            throw new RuntimeException("Error deleting key " + key, e);
        }
    }

    public KVPersistentStorage init(StorageConfig config){
        try {
            validate(config);
            this.internalStorage = new LSMPersistentStorage(config);
            internalStorage.init();
        } catch (IOException e) {
            throw new RuntimeException("Error initializing persistent storage", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private static void validate(StorageConfig config) throws IOException {
        Path baseDir = config.getDir();
        File baseFile = baseDir.toFile();
        if(!baseFile.exists()){
            throw new IOException("Base directory " + baseDir + " should exist - make sure it is created!!");
        }
        if(!baseFile.isDirectory()){
            throw new IOException("Base directory " + baseDir + " should be a directory!!");
        }
    }
}
