package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.SneakyThrows;
import org.rocksdb.WriteBatch;

public class RocksList<T> extends RocksMultiList<T> {

    public RocksList(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksList(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, name, valueType);
    }

    public void add(T value) {
        add(null, value);
    }

    public void addBatch(WriteBatch writeBatch, T... value) {
        addBatch(null, writeBatch, value);
    }

    @SneakyThrows
    public T get(long index) {
        return get(null, index);
    }

    @SneakyThrows
    public long size() {
        return size(null);
    }
}

