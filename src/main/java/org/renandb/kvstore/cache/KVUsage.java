package org.renandb.kvstore.cache;

import org.renandb.kvstore.KVPair;

public class KVUsage {

    private long time;
    private KVPair pair;

    public KVUsage(long time, KVPair kvPair) {
        this.time = time;
        this.pair = kvPair;
    }

    public Long getTime() {
        return time;
    }

    public KVPair getPair() {
        return pair;
    }
}
