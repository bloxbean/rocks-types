package com.bloxbean.rocks.types.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class JacksonSerializer implements Serializer {
    private ObjectMapper objectMapper;

    public JacksonSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    @SneakyThrows
    @Override
    public byte[] serialize(Object obj) {
        if (obj instanceof String s) {
            return s.getBytes();
        } else {
            return objectMapper.writeValueAsBytes(obj);
        }
    }

    @SneakyThrows
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz  ) {
        if (clazz == String.class) {
            return (T) new String(bytes);
        } else {
            return objectMapper.readValue(bytes, clazz);
        }
    }
}
