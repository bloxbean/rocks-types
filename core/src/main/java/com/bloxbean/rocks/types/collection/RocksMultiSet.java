package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.SetMetadata;
import com.bloxbean.rocks.types.collection.util.EmptyIterator;
import com.bloxbean.rocks.types.collection.util.ValueIterator;
import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

/**
 * Provides Set functionality on top of RocksDB. This is a multi set where you can have multiple sets
 * under the same name. Each set is identified by a namespace.
 */
public class RocksMultiSet<T> extends BaseDataType<T> {

    public RocksMultiSet(RocksDBConfig rocksDBConfig, String columnFamily, String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksMultiSet(RocksDBConfig rocksDBConfig, String name, Class<T> valueType) {
        super(rocksDBConfig, null, name, valueType);
    }

    @SneakyThrows
    public void add(byte[] ns, T member) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, member);
    }

    public void addBatch(byte[] ns, WriteBatch writeBatch, T... members) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var val : members) {
            add(ns, writeBatch, metadata, val);
        }
    }

    private void add(byte[] ns, WriteBatch writeBatch, SetMetadata metadata, T member) {
        write(writeBatch, getSubKey(metadata, ns, member), new byte[0]);
    }

    @SneakyThrows
    public boolean contains(byte[] ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return false;

        byte[] val = get(getSubKey(metadata.get(), ns, member));
        return val != null;
    }

    @SneakyThrows
    public void remove(byte[] ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;

        try(var writeBatch = new WriteBatch();
            var writeOption = new WriteOptions()) {
            delete(ns, writeBatch, metadata.get(), member);
            db.write(writeOption, writeBatch);
        }
    }

    @SneakyThrows
    public void removeBatch(byte[] ns, WriteBatch writeBatch, T... values) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;

        for (var value : values)
            delete(ns, writeBatch, metadata.get(), value);
    }

    private void delete(byte[] ns, WriteBatch writeBatch, SetMetadata metadata, T value) {
        deleteBatch(writeBatch, getSubKey(metadata, ns, value));
    }

    @SneakyThrows
    public Set<T> members(byte[] ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptySet();

        Set<T> members = new HashSet<>();
        byte[] prefix = getSubKey(metadata.get(), ns, null);
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!KeyBuilder.hasPrefix(key, prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }

                T member = getMemberFromCompositeSubKey(key);
                members.add(member);
            }
        }
        return members;
    }

    @SneakyThrows
    public ValueIterator<T> membersIterator(byte[] ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefix = getSubKey(metadata.get(), ns, null);
        return new SetIterator<>(iterator(), prefix);
    }

    @SneakyThrows
    protected Optional<SetMetadata> getMetadata(byte[] ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, SetMetadata.class));
        }
    }

    @Override
    protected Optional<SetMetadata> createMetadata(byte[] ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            var newMetadata = new SetMetadata();
            newMetadata.setVersion(System.currentTimeMillis());
            write(null, metadataKeyName, valueSerializer.serialize(newMetadata));
            return Optional.of(newMetadata);
        } else {
            return metadata;
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

    private byte[] getSubKey(SetMetadata metadata, byte[] ns, T member) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(metadata.getVersion())
                    .append(member != null ? valueSerializer.serialize(member) : null)
                    .build();
        else
            return new KeyBuilder(name)
                    .append(metadata.getVersion())
                    .append(member != null ? valueSerializer.serialize(member) : null)
                    .build();
    }

    private T getMemberFromCompositeSubKey(byte[] key) {
        var parts = KeyBuilder.decodeCompositeKey(key);
        return valueSerializer.deserialize(parts.get(parts.size() - 1), valueType);
    }

    private class SetIterator<T> implements ValueIterator<T> {
        private final RocksIterator iterator;
        private final byte[] prefix;

        public SetIterator(@NonNull RocksIterator rocksIterator,
                           @NonNull byte[] prefix) {
            this.iterator = rocksIterator;
            this.prefix = prefix;

            this.iterator.seek(prefix);
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid() && KeyBuilder.hasPrefix(iterator.key(), prefix);
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            iterator.next();
            return (T) getMemberFromCompositeSubKey(key);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

}

