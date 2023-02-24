package org.renandb.kvstore.cache;

import org.renandb.kvstore.KVPair;

import java.util.*;

public class KVCache {
    private final long maxSize;
    private long time = 0;
    private final Queue<KVUsage> queue;
    private final Map<String, KVUsage> usageMap;

    public KVCache(int maxSize) {
        this.maxSize = maxSize;
        Comparator<KVUsage> lruComparator = Comparator.comparing(KVUsage::getTime);
        this.queue = new PriorityQueue<>(maxSize, lruComparator);
        this.usageMap = new HashMap<>(maxSize);
    }

    public synchronized void put(KVPair kvPair) {
        if(isFull()){
            KVUsage usage = queue.remove();
            usageMap.remove(usage.getPair().getKey());
        }
        time++;
        KVUsage usage = new KVUsage(time, kvPair);
        queue.add(usage);
        usageMap.put(kvPair.getKey(), usage);
    }

    private boolean isFull() {
        return maxSize <= this.queue.size();
    }

    public synchronized void delete(String key) {
        KVUsage usage = usageMap.get(key);
        if(usage != null){
            queue.remove(usage);
            usageMap.remove(key);
        }
    }

    public synchronized Optional<KVPair> get(String key) {
        KVUsage usage = usageMap.get(key);
        if(usage != null){
            KVPair pair = usage.getPair();
            delete(pair.getKey());
            put(pair);
            return Optional.of(usage.getPair());
        }
        return Optional.empty();
    }
}
