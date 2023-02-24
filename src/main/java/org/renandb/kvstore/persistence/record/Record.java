package org.renandb.kvstore.persistence.record;

import org.renandb.kvstore.KVPair;

import java.io.Serializable;

public class Record implements Serializable {
    private static final long serialVersionUID = 85824334376015541L;

    private final String key;
    private final String value;
    private final boolean empty;

    private Record(String key, String value, boolean empty){
        this.key = key;
        this.value = value;
        this.empty = empty;
    }

    public static Record newEmpty(String key) {
        return new Record(key, null, true);
    }

    public static Record from(KVPair pair) {
        return new Record(pair.getKey(), pair.getValue(), false);
    }

    public String getKey() {
        return key;
    }

    public boolean isEmpty() {
        return empty;
    }

    public KVPair toPair() {
        return new KVPair(key, value);
    }

    public long getSizeInBytes() {
        // ~ 2 bytes per string char, 1 byte for boolean
        int charLength = (key.length() + (value != null ? value.length() : 1));
        return 2 * charLength + 1;
    }
}
