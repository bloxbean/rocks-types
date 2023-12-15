package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.config.RocksDBConfig;
import org.rocksdb.WriteBatch;

import java.util.Set;

/**
 * Provides Set functionality on top of RocksDB
 * @param <T>
 */
public class RocksSet<T> extends RocksMultiSet<T> {

    public RocksSet(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksSet(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, null, name, valueType);
    }

    public void add(T value) {
        add(null, value);
    }

    public void addBatch(WriteBatch writeBatch, T... value) {
        addBatch(null, writeBatch, value);
    }

    public boolean contains(T value) {
       return contains(null, value);
    }

    public void remove(T value) {
        remove(null, value);
    }

    public void removeBatch(WriteBatch writeBatch, T... values) {
        removeBatch(null, writeBatch, values);
    }

    public Set<T> members() {
        return members(null);
    }

}

