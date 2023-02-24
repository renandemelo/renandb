package org.renandb.kvstore.persistence;

import java.io.IOException;
import org.renandb.kvstore.persistence.record.Record;

import java.nio.file.Path;
import java.util.Optional;

public interface SSTableSegment {
    Optional<Record> retrieve(String key) throws IOException;
    boolean mightContain(String key);

    Path getFile();
}
