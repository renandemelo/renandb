package org.renandb.kvstore;

import org.renandb.kvstore.cache.KVCache;
import org.renandb.kvstore.config.StorageConfig;

import java.util.Optional;
public class KVStore {

    private final KVCache cache;
    private final KVPersistentStorage persistentStorage;

    public KVStore(StorageConfig config) {
        this.cache = new KVCache(config.getCacheSize());
        this.persistentStorage = new KVPersistentStorage().init(config);
    }

    public void put(KVPair kvPair) {
        this.cache.put(kvPair);
        this.persistentStorage.store(kvPair);
    }

    public void delete(String key) {
        this.cache.delete(key);
        this.persistentStorage.delete(key);
    }

    public Optional<KVPair> get(String key) {
        Optional<KVPair> pair = this.cache.get(key);
        if(pair.isPresent()) return pair;
        pair = this.persistentStorage.retrieve(key);
        if(pair.isPresent()){
            this.cache.put(pair.get());
            return pair;
        }
        return Optional.empty();
    }
}
