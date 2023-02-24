package org.renandb.kvstore.util;

import org.renandb.kvstore.KVPair;

import java.util.ArrayList;
import java.util.List;

public class KVUtil {

    public static List<KVPair> generateRandomPairs(int many) {
        List<KVPair> pairs = new ArrayList<>(many);
        int r = (int) (Math.random() * 100);
        for(int i = 0; i < many; i++){
            char firstChar = (char) ((r + i) % 127); // ASCII range
            String generatedString = firstChar + "-" + i + "-" + r + "-key";
            pairs.add(new KVPair(generatedString, generatedString + i + "-value"));
        }
        return pairs;
    }
}
