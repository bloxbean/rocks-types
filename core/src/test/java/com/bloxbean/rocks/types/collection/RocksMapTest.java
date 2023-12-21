package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksMapTest extends RocksBaseTest {

    @Test
    void put_get() {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        assertThat(map.get("key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get("key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get("key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get("key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get("key5")).isEqualTo(Optional.empty());
    }

    @Test
    void putBatch_get() throws Exception {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        var writeBatch = new WriteBatch();
        map.putBatch(writeBatch, new Tuple<>("key1", "value1"),
                new Tuple<>("key2", "value2"), new Tuple<>("key3", "value3"), new Tuple<>("key4", "value4"));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.get("key1")).isEqualTo(Optional.of("value1"));
        assertThat(map.get("key2")).isEqualTo(Optional.of("value2"));
        assertThat(map.get("key3")).isEqualTo(Optional.of("value3"));
        assertThat(map.get("key4")).isEqualTo(Optional.of("value4"));
        assertThat(map.get("key5")).isEqualTo(Optional.empty());
    }

    @Test
    void contains() {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        assertThat(map.contains("key1")).isTrue();
        assertThat(map.contains("key2")).isTrue();
        assertThat(map.contains("key3")).isTrue();
        assertThat(map.contains("key4")).isTrue();
        assertThat(map.contains("key5")).isFalse();
    }

    @Test
    void remove() {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        map.remove("key1");
        map.remove("key4");

        assertThat(map.contains("key1")).isFalse();
        assertThat(map.contains("key2")).isTrue();
        assertThat(map.contains("key3")).isTrue();
        assertThat(map.contains("key4")).isFalse();
    }

    @Test
    void removeBatch() throws Exception {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        var writeBatch = new WriteBatch();
        map.removeBatch(writeBatch, "key1", "key4");
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(map.contains("key1")).isFalse();
        assertThat(map.contains("key2")).isTrue();
        assertThat(map.contains("key3")).isTrue();
        assertThat(map.contains("key4")).isFalse();
    }

    @Test
    void entries() {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        var entries1 = map.entries();

        assertThat(entries1).isEqualTo(Set.of(Map.entry("key1", "value1"),
                Map.entry("key2", "value2"),
                Map.entry("key3", "value3"),
                Map.entry("key4", "value4")));

    }

    @Test
    void entries_iterator() throws Exception {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        var iterator = map.entriesIterator();

        var entries1 = new ArrayList<>();
        while(iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            entries1.add(entry);
        }
        iterator.close();

        assertThat(entries1).isEqualTo(List.of(Map.entry("key1", "value1"),
                Map.entry("key2", "value2"),
                Map.entry("key3", "value3"),
                Map.entry("key4", "value4")));

    }

    @Test
    void put_multiget() {
        var map = new RocksMap<String, String>(rocksDBConfig, "testMap", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        List<String> values = map.multiGet(List.of("key1", "key2", "key3", "key4"));
        assertThat(values).isEqualTo(List.of("value1", "value2", "value3", "value4"));
    }
}
