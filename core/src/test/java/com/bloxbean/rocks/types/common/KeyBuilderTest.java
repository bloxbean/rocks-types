package com.bloxbean.rocks.types.common;

import com.bloxbean.rocks.types.serializer.MessagePackSerializer;
import org.junit.jupiter.api.Test;

import static com.bloxbean.rocks.types.common.KeyBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

class KeyBuilderTest {

    @Test
    void build() {
        var keyBuilder = new KeyBuilder("zset1", "ns1");
        var key = keyBuilder
                .append("one")
                .append(1L)
                .append("two")
                .append(202L)
                .build();

        System.out.println(new String(key));

        var parts = KeyBuilder.decodeCompositeKey(key);
        parts.forEach(p -> System.out.println(new String(p)));
    }

    @Test
    void parts() {
        var keyBuilder = new KeyBuilder("zset1", "ns1");
        var key = keyBuilder
                .append("one")
                .append(1L)
                .append("two".getBytes())
                .append(202L)
                .build();

        var parts = KeyBuilder.decodeCompositeKey(key);
        assertThat(parts.size()).isEqualTo(6);
        assertThat(bytesToStr(parts.get(0))).isEqualTo("zset1");
        assertThat(bytesToStr(parts.get(1))).isEqualTo("ns1");
        assertThat(bytesToStr(parts.get(2))).isEqualTo("one");
        assertThat(bytesToLong(parts.get(3))).isEqualTo(1L);
        assertThat(parts.get(4)).isEqualTo("two".getBytes());
        assertThat(bytesToLong(parts.get(5))).isEqualTo(202L);
    }

    @Test
    void appendToKey() {
        var keyBuilder = new KeyBuilder("zset1", "ns1");
        var key = keyBuilder
                .append("one")
                .build();

        var finalKey = KeyBuilder.appendToKey(key, longToBytes(5L), intToBytes(2), strToBytes("hello"));

        var parts = KeyBuilder.decodeCompositeKey(finalKey);
        assertThat(parts.size()).isEqualTo(6);
        assertThat(bytesToStr(parts.get(0))).isEqualTo("zset1");
        assertThat(bytesToStr(parts.get(1))).isEqualTo("ns1");
        assertThat(bytesToStr(parts.get(2))).isEqualTo("one");
        assertThat(bytesToLong(parts.get(3))).isEqualTo(5L);
        assertThat(bytesToInt(parts.get(4))).isEqualTo(2);
        assertThat(bytesToStr(parts.get(5))).isEqualTo("hello");
    }

    @Test
    void compositeKey() {
        MessagePackSerializer valueSerializer = new MessagePackSerializer();
        String key = "key1";
        long currentTime = System.currentTimeMillis();
        var compositeKey = new KeyBuilder("testMap")
                .append(currentTime)
                .append(key != null? valueSerializer.serialize(key): null)
                .build();

        var parts = KeyBuilder.decodeCompositeKey(compositeKey);
        System.out.println(parts.size());

        assertThat(parts.size()).isEqualTo(3);
        assertThat(bytesToStr(parts.get(0))).isEqualTo("testMap");
        assertThat(bytesToLong(parts.get(1))).isEqualTo(currentTime);
        assertThat(valueSerializer.deserialize(parts.get(2), String.class)).isEqualTo(key);
    }
}
