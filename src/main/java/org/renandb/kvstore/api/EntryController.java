package org.renandb.kvstore.api;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.renandb.kvstore.KVPair;
import org.renandb.kvstore.KVStore;
import org.renandb.kvstore.config.StorageConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;


@RestController
@RequestMapping(path = "/entries")
public class EntryController
{
    private final StorageConfig config;
    private KVStore kvStore;

    public EntryController(StorageConfig config) throws IOException {
        this.config = config;
    }

    @PostConstruct
    private void init(){
        kvStore = new KVStore(config);
    }

    @GetMapping(path="/{key}", produces = "application/json")
    public ResponseEntity<Entry> getEntry(@PathVariable("key") String key)
    {
        Optional<KVPair> pair = kvStore.get(key);
        if(pair.isPresent()) return ResponseEntity.ok(Entry.from(pair.get()));
        return ResponseEntity.notFound().build();
    }

    /**
     * According to PUT semantics server MUST reply CREATED (201) if resource is new.
     * Here we obey these semantics with tradeoff of reducing dramatically write throughput.
     */
    @PutMapping(path= "/{key}", consumes = "application/json", produces = "application/json")
    public ResponseEntity<Entry> putEntry(@PathVariable("key") String key, @RequestBody Entry entry, HttpServletRequest request)
    {
        Optional<KVPair> previousPair = kvStore.get(key);
        KVPair pair = new KVPair(key, entry.getValue());
        kvStore.put(pair);
        URI resourceURI = URI.create(request.getRequestURI());
        if(previousPair.isEmpty()) {
            return ResponseEntity.created(resourceURI).build();
        }
        return ResponseEntity.ok(Entry.from(pair));
    }


    /**
     * Here we, in opposite to PUT we are taking some "freedom" in
     * RESTful semantics and reading values to allow much higher write throughput
     */
    @PostMapping(path= "/", consumes = "application/json", produces = "application/json")
    public ResponseEntity<Entry> postEntry(@RequestBody Entry entry)
    {
        kvStore.put(entry.toPair());
        return ResponseEntity.ok(entry);
    }

    @DeleteMapping(path="/{key}")
    public ResponseEntity<Void> deleteEntry(@PathVariable("key") String key)
    {
        if(kvStore.get(key).isPresent()){
            kvStore.delete(key);
            return ResponseEntity.ok().build();
        }
        return ResponseEntity.noContent().build();
    }
}