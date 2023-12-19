package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksMultiMapTest extends RocksBaseTest {

    @Test
    void put_get() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");

        assertThat(map.get("ns1", "key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get("ns1", "key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get("ns1", "key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get("ns1", "key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get("ns1", "key5")).isEqualTo(Optional.empty());
    }

    @Test
    void putBatch_get() throws Exception {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        var writeBatch = new WriteBatch();
        map.putBatch("ns1", writeBatch, new Tuple<>("key1", "value1"),
                new Tuple<>("key2", "value2"), new Tuple<>("key3", "value3"), new Tuple<>("key4", "value4"));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.get("ns1", "key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get("ns1", "key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get("ns1", "key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get("ns1", "key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get("ns1", "key5")).isEqualTo(Optional.empty());
    }

    @Test
    void contains() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");

        assertThat(map.contains("ns1", "key1")).isTrue();
        assertThat(map.contains("ns1", "key2")).isTrue();
        assertThat(map.contains("ns1", "key3")).isTrue();
        assertThat(map.contains("ns1", "key4")).isTrue();
        assertThat(map.contains("ns1", "key5")).isFalse();
    }

    @Test
    void remove() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");

        map.remove("ns1", "key1");
        map.remove("ns1", "key4");

        assertThat(map.contains("ns1", "key1")).isFalse();
        assertThat(map.contains("ns1", "key2")).isTrue();
        assertThat(map.contains("ns1", "key3")).isTrue();
        assertThat(map.contains("ns1", "key4")).isFalse();
    }

    @Test
    void removeBatch() throws Exception {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");

        var writeBatch = new WriteBatch();
        map.removeBatch("ns1", writeBatch, "key1", "key4");
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.contains("ns1", "key1")).isFalse();
        assertThat(map.contains("ns1", "key2")).isTrue();
        assertThat(map.contains("ns1", "key3")).isTrue();
        assertThat(map.contains("ns1", "key4")).isFalse();
    }

    @Test
    void entries() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");
        map.put("ns2", "key5", "value5");
        map.put("ns2", "key6", "value6");

        var entries1 = map.entries("ns1");
        var entries2 = map.entries("ns2");

        assertThat(entries1).isEqualTo(Set.of(Map.entry("key1", "value1"),
                Map.entry("key2", "value2"),
                Map.entry("key3", "value3"),
                Map.entry("key4", "value4")));

        assertThat(entries2).isEqualTo(Set.of(Map.entry("key5", "value5"),
                Map.entry("key6", "value6")));

    }

    @Test
    void entries_iterator() throws Exception {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("ns1", "key1", "value1");
        map.put("ns1", "key2", "value2");
        map.put("ns1", "key3", "value3");
        map.put("ns1", "key4", "value4");
        map.put("ns2", "key5", "value5");
        map.put("ns2", "key6", "value6");

        var iterator = map.entriesIterator("ns1");

        var entries1 = new ArrayList<>();
        while(iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            entries1.add(entry);
        }
        iterator.close();

        iterator = map.entriesIterator("ns2");
        var entries2 = new ArrayList<>();
        while(iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            entries2.add(entry);
        }

        assertThat(entries1).isEqualTo(List.of(Map.entry("key1", "value1"),
                Map.entry("key2", "value2"),
                Map.entry("key3", "value3"),
                Map.entry("key4", "value4")));

        assertThat(entries2).isEqualTo(List.of(Map.entry("key5", "value5"),
                Map.entry("key6", "value6")));
    }
}
