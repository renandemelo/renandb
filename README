# Overview
The application contains a REST API to handle persistently key-pair entries.

It uses a LRU-based (KVCache) and a Log-structured merge-based storage (LSM).

# Performance
Keys are stored persistently using a LSM-based persistence store (LSMPersistenceStorage) focused
on high performance for reads and writes - check MillionsOfRecordsStorageIntegrationTest.

The overall approach is explained in the book Designing Data-Intensive applications but
it simply fosters that sequential file access (e.g. append) is orders of magnitude faster than Random Access.

# Difference to the previous version
- Improved performance
    - BloomFilter (adding a combination of 3 hashes for more stable latency)
    - Using binary-search for finding chunk candidates.
- Restore from file system behavior implemented.

# Remaining work
These were not done due to time restrictions, but should be trivial:
## Compaction
Have an async job that merges segments and replace them in LSMPersistentStorage when ready.

## Crash-restore
Restore LSMPersistentStorage from files saved in baseDir:
- InMemorySSTAble can be fully restored from logFile
- FileBasedSSTable can be fully restored from segment directory

# How to run it? Create a directory to store the DB and then run:
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
