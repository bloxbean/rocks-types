package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.util.ValueIterator;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import org.rocksdb.WriteBatch;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provides Map functionality on top of RocksDB.
 * @param <K>
 * @param <V>
 */
public class RocksMap<K, V> extends RocksMultiMap<K, V> {

    public RocksMap(@NonNull RocksDBConfig rocksDBConfig, String columnFamily,
                    @NonNull String name, @NonNull Class<K> keyType, Class<V> valueType) {
        super(rocksDBConfig, columnFamily, name, keyType, valueType);
    }

    public RocksMap(RocksDBConfig rocksDBConfig, String name,
                    @NonNull Class<K> keyType, Class<V> valueType) {
        super(rocksDBConfig, name, keyType, valueType);
    }

    public void put(K key, V value) {
        super.put(null, key, value);
    }

    public void putBatch(WriteBatch writeBatch, Tuple<K, V>... keyValues) {
        super.putBatch(null, writeBatch, keyValues);
    }

    public Optional<V> get(K key) {
        return super.get(null, key);
    }

    public boolean contains(K key) {
        return super.contains(null, key);
    }

    public void remove(K key) {
        super.remove(null, key);
    }

    public void removeBatch(WriteBatch writeBatch, K... keys) {
        super.removeBatch(null, writeBatch, keys);
    }

    public Set<Map.Entry<K, V>> entries() {
        return super.entries(null);
    }

    public ValueIterator<Map.Entry<K, V>> entriesIterator() {
        return super.entriesIterator(null);
    }
}
