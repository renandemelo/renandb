package org.renandb.kvstore.api;

import org.renandb.kvstore.KVPair;

public class Entry {
    private String key;
    private String value;

    public Entry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static Entry from(KVPair pair) {
        return new Entry(pair.getKey(), pair.getValue());
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public KVPair toPair() {
        return new KVPair(key, value);
    }
}
