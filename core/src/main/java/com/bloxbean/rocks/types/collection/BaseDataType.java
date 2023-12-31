package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.TypeMetadata;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.SneakyThrows;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.util.List;
import java.util.Optional;

/**
 * Base class for all collection types
 * @param <T>
 */
abstract class BaseDataType<T> {
    protected final RocksDB db;
    protected final String name;
    protected final ColumnFamilyHandle columnFamilyHandle;
    protected final Serializer valueSerializer;
    protected final Class<T> valueType;

    public BaseDataType(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        this(rocksDBConfig, null, name, valueType);
    }

    public BaseDataType(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        this.db = rocksDBConfig.getRocksDB();
        this.name = name;
        this.valueSerializer = rocksDBConfig.getValueSerializer();
        if (columnFamily != null)
            this.columnFamilyHandle = rocksDBConfig.getColumnFamilyHandle(columnFamily);
        else
            this.columnFamilyHandle = null;
        this.valueType = valueType;
    }

    protected abstract Optional<? extends TypeMetadata> createMetadata(byte[] ns);
    protected abstract Optional<? extends TypeMetadata> getMetadata(byte[] ns);

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
    protected void merge(WriteBatch writeBatch, byte[] key, byte[] value) {
        if (writeBatch != null) {
            mergeBatch(writeBatch, key, value);
        } else {
            merge(key, value);
        }
    }

    @SneakyThrows
    protected void mergeBatch(WriteBatch batch, byte[] key, byte[] value) {
        if (columnFamilyHandle != null) {
            batch.merge(columnFamilyHandle, key, value);
        } else {
            batch.merge(key, value);
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
    protected List<byte[]> get(List<byte[]> keys) {
        if (columnFamilyHandle != null) {
            var columnFamilies = keys.stream()
                    .map(key -> columnFamilyHandle)
                    .toList();

            return db.multiGetAsList(columnFamilies, keys);
        } else {
            return db.multiGetAsList(keys);
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

    @SneakyThrows
    protected void merge(byte[] key, byte[] value) {
        if (columnFamilyHandle != null) {
            db.merge(columnFamilyHandle, key, value);
        } else {
            db.merge(key, value);
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
