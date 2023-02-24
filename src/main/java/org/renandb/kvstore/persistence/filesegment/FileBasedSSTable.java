package org.renandb.kvstore.persistence.filesegment;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.renandb.kvstore.persistence.SSTableSegment;
import org.renandb.kvstore.persistence.maintenance.Serializer;
import org.renandb.kvstore.persistence.record.Record;
import org.renandb.kvstore.persistence.record.RecordChunk;
import org.renandb.kvstore.util.FileUtil;

public class FileBasedSSTable implements SSTableSegment {

    private SSTableMetadata metadata;
    private Path contentFile;
    private Path metadataFile;
    private Path segmentDir;

    public FileBasedSSTable(Path dir) {
        this.segmentDir = dir;
        this.metadataFile = Path.of( segmentDir + File.separator + "metadata");
        this.contentFile = Path.of(segmentDir + File.separator + "content");
    }

    public FileBasedSSTable init() throws IOException, ClassNotFoundException {
        this.metadata = restoreMetadata();
        return this;
    }

    public FileBasedSSTable init(SSTableMetadata metadata) throws IOException {
        this.metadata = metadata;
        saveMetadata();
        return this;
    }

    private SSTableMetadata restoreMetadata() throws IOException, ClassNotFoundException {
        return (SSTableMetadata) Serializer.restore(Files.readAllBytes(this.metadataFile));
    }

    private void saveMetadata() throws IOException {
        Files.write(this.metadataFile, Serializer.serialize(this.metadata));
    }

    @Override
    public Optional<Record> retrieve(String key) throws IOException {
        ChunkLocation candidate = metadata.findCandidateFor(key);
        if(candidate != null){
            try(FileInputStream fis = new FileInputStream(contentFile.toFile())){
                RecordChunk chunk = readRecordChunk(candidate, fis);
                return chunk.get(key);
            }
        }
        return Optional.empty();
    }

    public RecordChunk readRecordChunk(ChunkLocation location, FileInputStream fis) throws IOException {
        byte[] chunkBytes = new byte[location.getSize()];
        fis.getChannel().position(location.getPosition());
        fis.read(chunkBytes);
        RecordChunk chunk = parseChunk(chunkBytes);
        return chunk;
    }

    private RecordChunk parseChunk(byte[] chunkBytes) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(chunkBytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            RecordChunk chunk = (RecordChunk) in.readObject();
            return chunk;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            in.close();
        }
    }

    public boolean mightContain(String key){
        return metadata.getBloomFilter().mightContain(key);
    }

    @Override
    public Path getFile() {
        return this.segmentDir;
    }

    public SSTableMetadata getMetadata(){
        return metadata;
    }

    public Path getContentFile() {
        return contentFile;
    }

    public void destroyFromDisk() throws IOException {
        FileUtil.deleteDir(this.segmentDir);
    }
}