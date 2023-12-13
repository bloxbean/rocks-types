package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.config.RocksDBConfig;
import org.rocksdb.WriteBatch;

import java.util.Set;

public class RocksSet extends RocksMultiSet {

    public RocksSet(RocksDBConfig rocksDBConfig, String columnFamily, String name) {
        super(rocksDBConfig, columnFamily, name);
    }

    public RocksSet(RocksDBConfig rocksDBConfig, String name) {
        super(rocksDBConfig, null, name);
    }

    public void add(String value) {
        add(null, value);
    }

    public void addBatch(WriteBatch writeBatch, String... value) {
        addBatch(null, writeBatch, value);
    }

    public boolean contains(String value) {
       return contains(null, value);
    }

    public void remove(String value) {
        remove(null, value);
    }

    public void removeBatch(WriteBatch writeBatch, String... values) {
        removeBatch(null, writeBatch, values);
    }

    public Set<String> members() {
        return members(null);
    }

}

