# Overview
The application contains a REST API to handle persistently key-pair entries.

It uses a LRU-based (KVCache) and a Log-structured merge-based storage (LSM).

# Overall behavior
The overall behavior is based on a LSM tree and can be found in articles in the internet (e.g. https://www.baeldung.com/cassandra-storage-engine).

## InMemorySSTable

It receives all changes to any key (including delete) and stores them sequentially into an append-only file + memory - ensuring fast writes/reads in this layer without affecting durability. When a threshold is reached, it converts asynchronously this level into a FileBasedSSTable segment.

## FileBasedSSTable
Each table segment contains data sorted 
- Metadata: A sorted sparse index with pointers to locations into the content file - it does not contain all keys but ideally points to a single disk block (4KB size). 
- Bloom filter: to avoid looking into segments without need.
- Content file: File with the key-value pairs (content).

## Compaction / Merging segments
The overall behavior stores every change operation to any key and therefore compaction is essential to guarantee long-term sustainability of the data storage. For that consecutive segments are merged (via a merge-sort similar merge algorithm) and only newer values for keys are kept - while older changes are discarded.
 
Since FileBasedSSTable segments are always imutable and therefore merging can be done asynchronously without blocks in the DB operation.

## Crash-restore
Restore LSMPersistentStorage from files saved in baseDir:
- InMemorySSTable can be fully restored from logFile
- FileBasedSSTable can be fully restored from segment directory

# Performance
Keys are stored persistently using a LSM-based persistence store (LSMPersistenceStorage) focused
on high performance for reads and writes - check MillionsOfRecordsStorageIntegrationTest.

The overall approach is explained in the book Designing Data-Intensive applications but
it simply fosters that sequential file access (e.g. append) to the file systems
is at least one or of magnitude faster than Random Disk Access, so it's possible to have a top-layer segment of the database purely based on an append-only + memory storage.

# How to run it?
Create a directory to store the DB and then run:
`mvn spring-boot:run -Dspring-boot.run.arguments="--storage.dir=/tmp/another-db --storage.cache-size=400"`

# How to test it?

- GET an entry from the store:
`curl -X GET  http://localhost:8080/entries/myKey`

- PUT a key into the store:
```
curl -X PUT http://localhost:8080/entries/myKey \
   -H 'Content-Type: application/json'\
   -d '{"value":"myValue"}'
```

- POST a key into the store - higher write throughput (but less REST-ful semantics):
```
curl -X POST http://localhost:8080/entries/ \
   -H 'Content-Type: application/json'\
   -d '{"key":"myKey","value":"myValue"}'
```

- DELETE entry from store:
```
curl -X DELETE http://localhost:8080/entries/myKey

```
