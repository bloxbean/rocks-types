package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.TypeMetadata;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.SneakyThrows;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.util.Optional;

abstract class BaseDataType<T> {
    protected final static String PREFIX = "|";
    protected final RocksDB db;
    protected final String name;
    protected final ColumnFamilyHandle columnFamilyHandle;
    protected final Serializer keySerializer;
    protected final Serializer valueSerializer;
    protected final Class<T> valueType;

    public BaseDataType(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        this(rocksDBConfig, null, name, valueType);
    }

    public BaseDataType(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        this.db = rocksDBConfig.getRocksDB();
        this.name = name;
        this.keySerializer = rocksDBConfig.getKeySerializer();
        this.valueSerializer = rocksDBConfig.getValueSerializer();
        if (columnFamily != null)
            this.columnFamilyHandle = rocksDBConfig.getColumnFamilyHandle(columnFamily);
        else
            this.columnFamilyHandle = null;
        this.valueType = valueType;
    }

    protected abstract Optional<? extends TypeMetadata> createMetadata(String ns);
    protected abstract Optional<? extends TypeMetadata> getMetadata(String ns);

    protected void write(WriteBatch writeBatch, byte[] key, byte[] value) {
        if (writeBatch != null) {
            writeBatch(writeBatch, key, value);
        } else {
            put(key, value);
        }
    }

    @SneakyThrows
    protected void writeBatch(WriteBatch batch, byte[] key, byte[] value) {
        if (columnFamilyHandle != null) {
            batch.put(columnFamilyHandle, key, value);
        } else {
            batch.put(key, value);
        }
    }

    @SneakyThrows
    protected void deleteBatch(WriteBatch batch, byte[] key) {
        if (columnFamilyHandle != null) {
            batch.delete(columnFamilyHandle, key);
        } else {
            batch.delete(key);
        }
    }

    @SneakyThrows
    protected void put(byte[] key, byte[] value) {
        if (columnFamilyHandle != null) {
            db.put(columnFamilyHandle, key, value);
        } else {
            db.put(key, value);
        }
    }

    @SneakyThrows
    protected byte[] get(byte[] key) {
        if (columnFamilyHandle != null) {
            return db.get(columnFamilyHandle, key);
        } else {
            return db.get(key);
        }
    }

    @SneakyThrows
    protected void delete(byte[] key) {
        if (columnFamilyHandle != null) {
            db.delete(columnFamilyHandle, key);
        } else {
            db.delete(key);
        }
    }

    protected RocksIterator iterator() {
        if (columnFamilyHandle != null) {
            return db.newIterator(columnFamilyHandle);
        } else {
            return db.newIterator();
        }
    }
}
