package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksMultiMapTest extends RocksBaseTest {
    byte[] ns = "ns1".getBytes();
    byte[] ns2 = "ns2".getBytes();

    @Test
    void put_get() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");

        assertThat(map.get(ns, "key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get(ns, "key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get(ns, "key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get(ns, "key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get(ns, "key5")).isEqualTo(Optional.empty());
    }

    @Test
    void putBatch_get() throws Exception {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        var writeBatch = new WriteBatch();
        map.putBatch(ns, writeBatch, new Tuple<>("key1", "value1"),
                new Tuple<>("key2", "value2"), new Tuple<>("key3", "value3"), new Tuple<>("key4", "value4"));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.get(ns, "key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get(ns, "key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get(ns, "key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get(ns, "key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get(ns, "key5")).isEqualTo(Optional.empty());
    }

    @Test
    void contains() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");

        assertThat(map.contains(ns, "key1")).isTrue();
        assertThat(map.contains(ns, "key2")).isTrue();
        assertThat(map.contains(ns, "key3")).isTrue();
        assertThat(map.contains(ns, "key4")).isTrue();
        assertThat(map.contains(ns, "key5")).isFalse();
    }

    @Test
    void remove() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");

        map.remove(ns, "key1");
        map.remove(ns, "key4");

        assertThat(map.contains(ns, "key1")).isFalse();
        assertThat(map.contains(ns, "key2")).isTrue();
        assertThat(map.contains(ns, "key3")).isTrue();
        assertThat(map.contains(ns, "key4")).isFalse();
    }

    @Test
    void removeBatch() throws Exception {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");

        var writeBatch = new WriteBatch();
        map.removeBatch(ns, writeBatch, "key1", "key4");
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.contains(ns, "key1")).isFalse();
        assertThat(map.contains(ns, "key2")).isTrue();
        assertThat(map.contains(ns, "key3")).isTrue();
        assertThat(map.contains(ns, "key4")).isFalse();
    }

    @Test
    void entries() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");
        map.put(ns2, "key5", "value5");
        map.put(ns2, "key6", "value6");

        var entries1 = map.entries(ns);
        var entries2 = map.entries(ns2);

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
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");
        map.put(ns2, "key5", "value5");
        map.put(ns2, "key6", "value6");

        var iterator = map.entriesIterator(ns);

        var entries1 = new ArrayList<>();
        while(iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            entries1.add(entry);
        }
        iterator.close();

        iterator = map.entriesIterator(ns2);
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

    @Test
    void put_multiget() {
        var map = new RocksMultiMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put(ns, "key1", "value1");
        map.put(ns, "key2", "value2");
        map.put(ns, "key3", "value3");
        map.put(ns, "key4", "value4");

        List<String> values = map.multiGet(ns, List.of("key1", "key2", "key3", "key4"));
        assertThat(values).isEqualTo(List.of("value1", "value2", "value3", "value4"));
    }
}
