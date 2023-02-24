package org.renandb.kvstore;

import java.util.Objects;

public class KVPair {

    public static final String EMPTY_VALUE = "EMPTY_VALUE";
    private final String key;
    private final String value;

    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVPair kvPair = (KVPair) o;
        return Objects.equals(key, kvPair.key) && Objects.equals(value, kvPair.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KVPair{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
