package org.renandb.kvstore.persistence;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.*;

public class Serializer {

    private static ObjectMapper MAPPER;
    private static final ObjectWriter objectWriter;
    private static final ObjectReader objectReader;

    static {
        SmileFactory factory = new SmileFactory();
        MAPPER = new ObjectMapper(factory);
        factory.setCodec(MAPPER);
        objectReader = MAPPER.reader();
        objectWriter = MAPPER.writer();
    }
    public static byte[] serialize(Object object) throws IOException {
        byte[] bytes = objectWriter.writeValueAsBytes(object);
        return bytes;
    }

    public static <T> T restore(byte[] readAllBytes, Class<T> type) throws IOException {
        return objectReader.readValue(readAllBytes, type);
    }
}
