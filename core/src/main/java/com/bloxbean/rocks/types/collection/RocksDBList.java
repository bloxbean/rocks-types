package com.bloxbean.rocks.types.collection;

import com.bloxbean.cardano.yaci.store.rocksdb.config.RocksDBConfig;
import com.bloxbean.cardano.yaci.store.rocksdb.types.metadata.ListMetadata;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.WriteBatch;

public class RocksDBList<T> extends BaseDataType<T> {

    public RocksDBList(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksDBList(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, name, valueType);
    }
//    private final static String PREFIX = ":";
//    private final RocksDB db;
//    private final String name;
//    private final byte[] metadataKeyName;
//    private final ColumnFamilyHandle columnFamilyHandle;
//    private final Serializer keySerializer;
//    private final Serializer valueSerializer;
//    private final Class<T> valueType;

//    public RocksDBList(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
//        this(rocksDBConfig, null, name, valueType);
//    }
//
//    public RocksDBList(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
//        this.db = rocksDBConfig.getRocksDB();
//        this.name = name;
//        this.keySerializer = rocksDBConfig.getKeySerializer();
//        this.valueSerializer = rocksDBConfig.getValueSerializer();
//        this.metadataKeyName = keySerializer.serialize(name);
//        if (columnFamily != null)
//            this.columnFamilyHandle = rocksDBConfig.getColumnFamilyHandle(columnFamily);
//        else
//            this.columnFamilyHandle = null;
//        this.valueType = valueType;
//    }

    public void add(T value) {
        var metadata = getMetadata().orElse(null);
        add(null, metadata, value);
    }

    public void addBatch(WriteBatch writeBatch, T... value) {
        var metadata = getMetadata().orElse(null);
        for (T val : value)
            add(writeBatch, metadata, val);
    }

    @SneakyThrows
    private void add(WriteBatch writeBatch, @NonNull ListMetadata metadata, T value) {
        long index = metadata.getSize(); // Get the current length of the list
        byte[] key = keySerializer.serialize(getSubKey(metadata, index));
        write(writeBatch, key, valueSerializer.serialize(value));
        updateMetadata(writeBatch, metadata, key); // Increment the length

//        else {
//            var metadata = createMetadata(writeBatch, name);
//            long index = 0; // Get the current length of the list
//            byte[] key = keySerializer.serialize(getSubKey(metadata, index));
//            write(writeBatch, columnFamilyHandle, key, valueSerializer.serialize(value));
//            updateMetadata(writeBatch, key); // Increment the length
//        }
    }

    @SneakyThrows
    public T get(long index) {
        var metadataOpt = getMetadata();
        if (metadataOpt.isPresent()) {
            var currentMetadata = metadataOpt.get();
            byte[] value = get(keySerializer.serialize(getSubKey(currentMetadata, index)));
            return value != null ? valueSerializer.deserialize(value, valueType) : null;
        } else {
            return null;
        }
    }

    @SneakyThrows
    public long size() {
        var metadata = getMetadata();
        if (metadata.isPresent()) {
            return metadata.get().getSize();
        } else {
            return 0;
        }
    }

    private String getSubKey(ListMetadata currentMetadata, long index) {
        return name + PREFIX + currentMetadata.getVersion() + PREFIX + index;
    }

    @SneakyThrows
    private ListMetadata updateMetadata(WriteBatch writeBatch, ListMetadata metadata, byte[] currentKey) {
        metadata.setSize(metadata.getSize() + 1);
        metadata.setTail(currentKey);
        write(writeBatch, metadataKeyName, valueSerializer.serialize(metadata));
        return metadata;

//        var metadataValueBytes = db.get(columnFamilyHandle, metadataKeyName);
//
//        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
//            var metadata = new ListMetadata();
//            metadata.setSize(1);
//            metadata.setHead(currentKey);
//            metadata.setTail(currentKey);
//            write(writeBatch, columnFamilyHandle, metadataKeyName, valueSerializer.serialize(metadata));
//            return metadata;
//        } else {
//            var metadata = valueSerializer.deserialize(metadataValueBytes, ListMetadata.class);
//            metadata.setSize(metadata.getSize() + 1);
//            metadata.setTail(currentKey);
//            write(writeBatch, columnFamilyHandle, metadataKeyName, valueSerializer.serialize(metadata));
//            return metadata;
//        }
    }

//    private ListMetadata createMetadata(WriteBatch writeBatch, String name) {
//        var metadata = new ListMetadata();
//        metadata.setSize(0);
//        metadata.setVersion(System.currentTimeMillis());
//        write(writeBatch, columnFamilyHandle, metadataKeyName, valueSerializer.serialize(metadata));
//        return metadata;
//    }

//    @SneakyThrows
//    private Optional<ListMetadata> getMetadata() {
//        var metadataValueBytes = db.get(columnFamilyHandle, metadataKeyName);
//        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
//            var listMetadata = new ListMetadata();
//            listMetadata.setSize(0);
//            listMetadata.setVersion(System.currentTimeMillis());
//            return Optional.of(listMetadata);
//        } else {
//            return Optional.of(valueSerializer.deserialize(metadataValueBytes, ListMetadata.class));
//        }
//    }
//
//    private void write(WriteBatch writeBatch, ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
//        if (writeBatch != null) {
//            writeBatch(writeBatch, columnFamilyHandle, key, value);
//        } else {
//            put(columnFamilyHandle, key, value);
//        }
//    }
//
//    @SneakyThrows
//    private void writeBatch(WriteBatch batch, ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
//        if (columnFamilyHandle != null) {
//            batch.put(columnFamilyHandle, key, value);
//        } else {
//            batch.put(key, value);
//        }
//    }
//
//    @SneakyThrows
//    private void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) {
//        if (columnFamilyHandle != null) {
//            db.put(columnFamilyHandle, key, value);
//        } else {
//            db.put(key, value);
//        }
//    }
}

