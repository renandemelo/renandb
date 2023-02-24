package org.renandb.kvstore.persistence.maintenance;

import org.renandb.kvstore.persistence.filesegment.FileBasedSSTable;
import org.renandb.kvstore.persistence.record.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeHandler {

    private final FileBasedSSTableReader reader1;
    private final FileBasedSSTableReader reader2;

    MergeHandler(FileBasedSSTable table1, FileBasedSSTable table2){
        this.reader1 = new FileBasedSSTableReader(table1);
        this.reader2 = new FileBasedSSTableReader((table2));
    }
    public boolean hasNext() throws IOException {
        return reader1.hasNext() || reader2.hasNext();
    }

    public void init() throws IOException {
        this.reader1.init();
        this.reader2.init();
    }

    public List<Record> getNext(int numberOfRecords) throws IOException {
        List<Record> sortedRecords = new ArrayList<>(numberOfRecords);
        int count = 0;
        while(count < numberOfRecords && (reader1.hasNext() || reader2.hasNext())){
            Record record1 = reader1.offer();
            Record record2 = reader2.offer();
            if(record2 == null){
                sortedRecords.add(record1);
                reader1.next();
            } else if (record1 == null) {
                sortedRecords.add(record2);
                reader2.next();
            } else{
                int comparison = record1.getKey().compareTo(record2.getKey());
                if(comparison == 0){
                    // Equals mean record1 should overwrite record2
                    sortedRecords.add(record1);
                    reader1.next();
                    reader2.next();
                } else if(comparison < 0){
                    sortedRecords.add(record1);
                    reader1.next();
                } else {
                    sortedRecords.add(record2);
                    reader2.next();
                }
            }
            count++;
        }
        return sortedRecords;
    }
}
