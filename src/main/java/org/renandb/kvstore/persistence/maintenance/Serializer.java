package org.renandb.kvstore.persistence.maintenance;

import java.io.*;

public class Serializer {

    public static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(object);
        out.flush();
        bos.flush();
        return bos.toByteArray();
    }

    public static Object restore(byte[] readAllBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream input = new ByteArrayInputStream(readAllBytes);
        ObjectInputStream ois = new ObjectInputStream(input);
        Object object = ois.readObject();
        try{
            return object;
        }finally {
            input.close();
            ois.close();
        }
    }
}
