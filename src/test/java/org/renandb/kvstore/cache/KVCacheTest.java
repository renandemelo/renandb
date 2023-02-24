package org.renandb.kvstore.cache;

import org.renandb.kvstore.KVPair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KVCacheTest {

    private KVCache cache;

    @Before
    public void prepareCache(){
        this.cache = new KVCache(3);
    }

    @Test
    public void aKeyJustIncludedIsFoundInTheCache(){
        cache.put(new KVPair("A", "2"));
        Assert.assertEquals(new KVPair("A", "2"), cache.get("A").get());
    }

    @Test
    public void aKeyUsedLongAgoIsNotFoundInTheCacheAnymore(){
        cache.put(new KVPair("A", "2"));
        cache.put(new KVPair("B", "3"));
        cache.put(new KVPair("C", "4"));
        cache.put(new KVPair("D", "5"));
        assertTrue(cache.get("A").isEmpty());
        Assert.assertEquals(new KVPair("B", "3"), cache.get("B").get());
    }

    @Test
    public void aKeyFetchedHasRefreshedUsage(){
        cache.put(new KVPair("A", "2"));
        cache.put(new KVPair("B", "3"));
        cache.put(new KVPair("C", "4"));
        cache.get("A"); // refreshes usage!
        cache.put(new KVPair("D", "5"));
        Assert.assertEquals(new KVPair("A", "2"), cache.get("A").get());
        assertTrue(cache.get("B").isEmpty());
    }
}
