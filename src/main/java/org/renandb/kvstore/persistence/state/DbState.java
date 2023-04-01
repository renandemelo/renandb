package org.renandb.kvstore.persistence.state;

import org.renandb.kvstore.persistence.SSTableSegment;

import java.beans.Transient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DbState {

    private transient Path file;

    private List<SegmentReference> segmentReferenceList = new ArrayList<>();

    public static DbState from(List<SSTableSegment> segments) {
        List<SegmentReference> segmentReferenceList = segments.stream().map(s -> SegmentReference.from(s)).collect(Collectors.toList());

        return new DbState().setSegmentReferenceList(segmentReferenceList);
    }

    public List<SegmentReference> getSegmentReferenceList() {
        return segmentReferenceList;
    }

    public DbState setSegmentReferenceList(List<SegmentReference> segmentReferenceList) {
        this.segmentReferenceList = segmentReferenceList;
        return this;
    }

    @Transient
    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        this.file = file;
    }

}
