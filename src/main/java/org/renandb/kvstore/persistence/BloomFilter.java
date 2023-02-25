package org.renandb.kvstore.persistence;

import java.io.Serializable;

public class BloomFilter implements Serializable {

    private static final long serialVersionUID = 85824334565656561L;
    private int[] filters = new int[30];
    public BloomFilter(){}
    public BloomFilter(BloomFilter bloomFilter1, BloomFilter bloomFilter2) {
        int[] filters1 = bloomFilter1.getFilters();
        int[] filters2 = bloomFilter2.getFilters();
        for(int i = 0; i < filters1.length; i++){
            filters[i] = filters1[i] | filters2[i];
        }
    }

    public int[] getFilters() {
        return this.filters;
    }
    public void setFilters(int[] filters) {
        this.filters = filters;
    }

    public synchronized boolean mightContain(String key){
        for(int i = 0; i < filters.length; i++){
            int hashValue = hash(key, i);
            if((filters[i] & hashValue) != hashValue)
                return false;
        }
        return true;
    }
    public synchronized BloomFilter register(String key){
        for(int i = 0; i < filters.length; i++){
            int hashValue = hash(key, i);
            filters[i] = filters[i] | hashValue;
        }
        return this;
    }

    private int hash(String key, int salt) {
        return (key + salt).hashCode();
    }

}
