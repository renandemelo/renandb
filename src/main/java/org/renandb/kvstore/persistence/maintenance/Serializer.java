package org.renandb.kvstore.persistence.maintenance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.*;

public class Serializer {

    private static ObjectMapper MAPPER;
    private static final ObjectWriter objectWriter;
    private static final ObjectReader objectReader;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.registerModules(new JavaTimeModule());
        MAPPER.enableDefaultTyping();
        objectReader = MAPPER.reader();
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
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
