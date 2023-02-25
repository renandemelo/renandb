package org.renandb.kvstore.persistence.record;

import org.renandb.kvstore.KVPair;

//import java.io.Serializable;

public class Record{
    private static final long serialVersionUID = 85824334376015541L;

    private String key;
    private String value;
    private boolean empty;

    public Record(){};
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


    public String getValue(){return value;}
    public boolean isEmpty() {
        return empty;
    }

    public KVPair toPair() {
        return new KVPair(key, value);
    }

    public long sizeInBytes() {
        // ~ 2 bytes per string char, 1 byte for boolean
        int charLength = (key.length() + (value != null ? value.length() : 1));
        return 2 * charLength + 1;
    }
}
