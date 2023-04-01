package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.filebased.FileBasedSSTable;

import java.io.IOException;

public interface MergedSegmentReceiver {
    boolean onMergedSegmentAvailable(FileBasedSSTable merged, FileBasedSSTable first, FileBasedSSTable second) throws IOException;
}
