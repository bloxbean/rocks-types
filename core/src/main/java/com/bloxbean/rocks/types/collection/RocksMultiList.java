package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.ListMetadata;
import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.WriteBatch;

import java.util.Optional;

/**
 * Provides List functionality on top of RocksDB. This is a multi list where you can have multiple lists
 * under the same name. Each list is identified by a namespace.
 * @param <T>
 */
public class RocksMultiList<T> extends BaseDataType<T> {

    public RocksMultiList(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksMultiList(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, name, valueType);
    }

    public void add(byte[] ns, T value) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, value);
    }

    public void addBatch(byte[] ns, WriteBatch writeBatch, T... value) {
        var metadata = createMetadata(ns).orElseThrow();
        for (T val : value)
            add(ns, writeBatch, metadata, val);
    }

    @SneakyThrows
    private void add(byte[] ns, WriteBatch writeBatch, @NonNull ListMetadata metadata, T value) {
        long index = metadata.getSize(); // Get the current length of the list
        byte[] key = getSubKey(metadata, ns, index);
        write(writeBatch, key, valueSerializer.serialize(value));
        updateMetadata(writeBatch, metadata, ns, key); // Increment the length
    }

    @SneakyThrows
    public T get(byte[] ns, long index) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return null;

        byte[] value = get(getSubKey(metadata.get(), ns, index));
        return value != null ? valueSerializer.deserialize(value, valueType) : null;
    }

    @SneakyThrows
    public long size(byte[] ns) {
        var metadata = getMetadata(ns);
        if (metadata.isPresent()) {
            return metadata.get().getSize();
        } else {
            return 0;
        }
    }

    @Override
    protected Optional<ListMetadata> createMetadata(byte[] ns) {
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
    private ListMetadata updateMetadata(WriteBatch writeBatch, ListMetadata metadata, byte[] ns, byte[] currentKey) {
        var metadataKeyName = getMetadataKey(ns);
        metadata.setSize(metadata.getSize() + 1);
        metadata.setTail(currentKey);
        write(writeBatch, metadataKeyName, valueSerializer.serialize(metadata));
        return metadata;
    }

    @SneakyThrows
    protected Optional<ListMetadata> getMetadata(byte[] ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, ListMetadata.class));
        }
    }

    protected byte[] getMetadataKey(byte[] ns) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .build();
        else
            return new KeyBuilder(name)
                    .build();
    }

    private byte[] getSubKey(ListMetadata currentMetadata, byte[] ns, long index) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(currentMetadata.getVersion())
                    .append(index)
                    .build();
        else
            return new KeyBuilder(name)
                    .append(currentMetadata.getVersion())
                    .append(index)
                    .build();
    }
}

