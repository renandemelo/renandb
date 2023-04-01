package org.renandb.kvstore.persistence.state;


import org.renandb.kvstore.persistence.memory.InMemorySSTable;
import org.renandb.kvstore.persistence.SSTableSegment;

public class SegmentReference {

    private SegmentType type;
    private String basePath;

    public SegmentReference(){

    }

    public SegmentReference(SegmentType type, String basePath){
        this.type = type;
        this.basePath = basePath;
    }
    public static SegmentReference from(SSTableSegment s) {
        SegmentReference reference = new SegmentReference();
        reference.setType(s instanceof InMemorySSTable? SegmentType.IN_MEMORY : SegmentType.FILE_BASED);
        reference.setBasePath(s.getFile().toString());
        return reference;
    }

    public SegmentType getType() {
        return type;
    }

    public void setType(SegmentType type) {
        this.type = type;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
}
