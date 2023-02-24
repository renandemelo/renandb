package org.renandb.kvstore.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
@ConfigurationProperties(prefix = "storage")
@ConfigurationPropertiesScan
public class StorageConfig {

    private boolean asyncProcessesActive = true;
    private Path dir;
    private int size;


    public boolean isSyncProcessesActive(){
        return asyncProcessesActive;
    }

    public StorageConfig setAsyncProcessesActive(boolean asyncProcessesActive){
        this.asyncProcessesActive = asyncProcessesActive;
        return this;
    }
    public Path getDir(){
        return dir;
    }

    public StorageConfig setDir(String dir){
        this.dir = Path.of(dir);
        return this;
    }

    public StorageConfig setCacheSize(int size){
        this.size = size;
        return this;
    }
    public int getCacheSize() {
        return size;
    }
}
