package com.bloxbean.rocks.types.collection;

import com.bloxbean.cardano.yaci.store.rocksdb.config.RocksDBConfig;
import com.bloxbean.cardano.yaci.store.rocksdb.serializer.Serializer;
import com.bloxbean.cardano.yaci.store.rocksdb.types.metadata.ListMetadata;
import lombok.SneakyThrows;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;

import java.util.Optional;

public abstract class BaseDataType<T> {
    protected final static String PREFIX = ":";
    protected final RocksDB db;
    protected final String name;
    protected final byte[] metadataKeyName;
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
        this.metadataKeyName = keySerializer.serialize(name);
        if (columnFamily != null)
            this.columnFamilyHandle = rocksDBConfig.getColumnFamilyHandle(columnFamily);
        else
            this.columnFamilyHandle = null;
        this.valueType = valueType;
    }


   // protected abstract RocksDB getDb();
    //protected abstract ColumnFamilyHandle getColumnFamilyHandle();
   // protected abstract byte[] getMetadataKeyName();

    @SneakyThrows
    protected Optional<ListMetadata> getMetadata() {
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            var listMetadata = new ListMetadata();
            listMetadata.setSize(0);
            listMetadata.setVersion(System.currentTimeMillis());
            return Optional.of(listMetadata);
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, ListMetadata.class));
        }
    }

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
}
