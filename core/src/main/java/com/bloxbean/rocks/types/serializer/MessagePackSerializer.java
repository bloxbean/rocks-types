package com.bloxbean.rocks.types.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.msgpack.jackson.dataformat.MessagePackMapper;

import java.nio.charset.StandardCharsets;

public class MessagePackSerializer implements Serializer {
    private ObjectMapper objectMapper;

    public MessagePackSerializer() {
        this.objectMapper = new MessagePackMapper().handleBigDecimalAsString();
    }

    @SneakyThrows
    @Override
    public byte[] serialize(Object obj) {
        if (obj instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        } else {
            return objectMapper.writeValueAsBytes(obj);
        }
    }

    @SneakyThrows
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz  ) {
        if (clazz == String.class) {
            return (T) new String(bytes, StandardCharsets.UTF_8);
        } else {
            return objectMapper.readValue(bytes, clazz);
        }
    }
}
