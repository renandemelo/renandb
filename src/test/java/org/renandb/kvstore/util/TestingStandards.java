package org.renandb.kvstore.util;

import org.renandb.kvstore.config.StorageConfig;

public class TestingStandards {

    public static StorageConfig getDefaultConfig(){
        return new StorageConfig().setAsyncProcessesActive(false);
    }
}
