package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;

import java.io.IOException;

public interface MergedSegmentReceiver {
    boolean onMergedSegmentAvailable(FileBasedSSTable merged, FileBasedSSTable first, FileBasedSSTable second) throws IOException;
}
