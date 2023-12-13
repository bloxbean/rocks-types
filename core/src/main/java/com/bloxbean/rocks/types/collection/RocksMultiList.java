package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.ListMetadata;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.WriteBatch;

import java.util.Optional;

public class RocksMultiList<T> extends BaseDataType<T> {

    public RocksMultiList(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksMultiList(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, name, valueType);
    }

    public void add(String ns, T value) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, value);
    }

    public void addBatch(String ns, WriteBatch writeBatch, T... value) {
        var metadata = createMetadata(ns).orElseThrow();
        for (T val : value)
            add(ns, writeBatch, metadata, val);
    }

    @SneakyThrows
    private void add(String ns, WriteBatch writeBatch, @NonNull ListMetadata metadata, T value) {
        long index = metadata.getSize(); // Get the current length of the list
        byte[] key = keySerializer.serialize(getSubKey(metadata, ns, index));
        write(writeBatch, key, valueSerializer.serialize(value));
        updateMetadata(writeBatch, metadata, ns, key); // Increment the length
    }

    @SneakyThrows
    public T get(String ns, long index) {
        var metadataOpt = getMetadata(ns).orElseThrow();
        var currentMetadata = metadataOpt;
        byte[] value = get(keySerializer.serialize(getSubKey(currentMetadata, ns, index)));
        return value != null ? valueSerializer.deserialize(value, valueType) : null;
    }

    @SneakyThrows
    public long size(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isPresent()) {
            return metadata.get().getSize();
        } else {
            return 0;
        }
    }

    @Override
    protected Optional<ListMetadata> createMetadata(String ns) {
        var metadataKeyName = getMetadataKey(ns);
        var metadata = getMetadata(ns);
        if (!metadata.isPresent()) {
            var listMetadata = new ListMetadata();
            listMetadata.setSize(0);
            listMetadata.setVersion(System.currentTimeMillis());
            write(null, metadataKeyName, valueSerializer.serialize(listMetadata));
            return Optional.of(listMetadata);
        } else {
            return metadata;
        }
    }

    @SneakyThrows
    private ListMetadata updateMetadata(WriteBatch writeBatch, ListMetadata metadata, String ns, byte[] currentKey) {
        var metadataKeyName = getMetadataKey(ns);
        metadata.setSize(metadata.getSize() + 1);
        metadata.setTail(currentKey);
        write(writeBatch, metadataKeyName, valueSerializer.serialize(metadata));
        return metadata;
    }

    @SneakyThrows
    protected Optional<ListMetadata> getMetadata(String ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, ListMetadata.class));
        }
    }

    protected byte[] getMetadataKey(String ns) {
        if (ns != null)
            return keySerializer.serialize(name + PREFIX + ns);
        else
            return keySerializer.serialize(name);
    }

    private String getSubKey(ListMetadata currentMetadata, String ns, long index) {
        if (ns != null)
            return name + PREFIX + ns + PREFIX + currentMetadata.getVersion() + PREFIX + index;
        else
            return name + PREFIX + currentMetadata.getVersion() + PREFIX + index;
    }
}

