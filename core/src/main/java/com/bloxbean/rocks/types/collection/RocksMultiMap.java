package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.MapMetadata;
import com.bloxbean.rocks.types.collection.util.EmptyIterator;
import com.bloxbean.rocks.types.collection.util.ValueIterator;
import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

/**
 * Provides Map functionality on top of RocksDB. This is a multi map where you can have multiple maps
 * under the same name. Each map is identified by a namespace.
 * @param <K>
 * @param <V>
 */
public class RocksMultiMap<K, V> extends BaseDataType<V> {
    private final Class<K> keyType;

    public RocksMultiMap(@NonNull RocksDBConfig rocksDBConfig, String columnFamily,
                          @NonNull String name, @NonNull Class<K> keyType, @NonNull Class<V> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
        this.keyType = keyType;
    }

    public RocksMultiMap(@NonNull RocksDBConfig rocksDBConfig, @NonNull String name,
                         @NonNull Class<K> keyType, @NonNull Class<V> valueType) {
        super(rocksDBConfig, name, valueType);
        this.keyType = keyType;
    }

    public void put(String ns, K key, V value) {
        var metadata = createMetadata(ns).orElseThrow();
        put(ns, null, metadata, key, value);
    }

    public void putBatch(String ns, WriteBatch writeBatch, Tuple<K, V>... keyValues) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var keyVal : keyValues) {
            put(ns, writeBatch, metadata, keyVal._1, keyVal._2);
        }
    }

    public Optional<V> get(String ns, K key) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Optional.empty();

        byte[] valueBytes = get(getKey(metadata.get(), ns, key));
        if (valueBytes == null || valueBytes.length == 0)
            return Optional.empty();

        return Optional.of(valueSerializer.deserialize(valueBytes, valueType));
    }

    public List<V> multiGet(String ns, List<K> keys) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptyList();

        var keysBytes = keys.stream()
                .map(key -> getKey(metadata.get(), ns, key))
                .toList();

        List<byte[]> values = get(keysBytes);

        if (values == null || values.size() == 0)
            return Collections.emptyList();

        return values.stream()
                .map(value -> {
                    if (value == null)
                        return null;
                    else
                        return valueSerializer.deserialize(value, valueType);
                })
                .toList();
    }

    public boolean contains(String ns, K key) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return false;

        byte[] valueBytes = get(getKey(metadata.get(), ns, key));
        return valueBytes != null && valueBytes.length > 0;
    }

    @SneakyThrows
    public void remove(String ns, K key) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;

        WriteBatch writeBatch = new WriteBatch();
        delete(ns, writeBatch, metadata.get(), key);
        db.write(new WriteOptions(), writeBatch);
    }

    @SneakyThrows
    public void removeBatch(String ns, WriteBatch writeBatch, K... keys) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;

        for (var key : keys)
            delete(ns, writeBatch, metadata.get(), key);
    }

    public Set<Map.Entry<K, V>> entries(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptySet();

        Set<Map.Entry<K, V>> members = new HashSet<>();
        byte[] prefix = getKey(metadata.get(), ns, null);
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!KeyBuilder.hasPrefix(key, prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }

                byte[] valueBytes = iterator.value();

                var keyBytes = KeyBuilder.removePrefix(key, prefix);
                K keyObj = valueSerializer.deserialize(keyBytes, keyType);
                V valueObj = valueSerializer.deserialize(valueBytes, valueType);

                members.add(new AbstractMap.SimpleEntry<>(keyObj, valueObj));
            }
        }
        return members;
    }

    public ValueIterator<Map.Entry<K, V>> entriesIterator(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefix = getKey(metadata.get(), ns, null);
        return new MapIterator(prefix);
    }

    private void put(String ns, WriteBatch writeBatch, MapMetadata metadata, K key, V value) {
        byte[] keyBytes = getKey(metadata, ns, key);
        byte[] valueBytes = valueSerializer.serialize(value);

        write(writeBatch, keyBytes, valueBytes);
    }

    private void delete(String ns, WriteBatch writeBatch, MapMetadata metadata, K key) {
        deleteBatch(writeBatch, getKey(metadata, ns, key));
    }

    @SneakyThrows
    protected Optional<MapMetadata> getMetadata(String ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, MapMetadata.class));
        }
    }

    @Override
    protected Optional<MapMetadata> createMetadata(String ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            var newMetadata = new MapMetadata();
            newMetadata.setVersion(System.currentTimeMillis());
            write(null, metadataKeyName, valueSerializer.serialize(newMetadata));
            return Optional.of(newMetadata);
        } else {
            return metadata;
        }
    }

    protected byte[] getMetadataKey(String ns) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .build();
        else
            return new KeyBuilder(name)
                    .build();
    }

    private byte[] getKey(MapMetadata metadata, String ns, K key) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(metadata.getVersion())
                    .append(key != null? valueSerializer.serialize(key): null)
                    .build();
        else
            return new KeyBuilder(name)
                    .append(metadata.getVersion())
                    .append(key != null? valueSerializer.serialize(key): null)
                    .build();
    }

    private class MapIterator implements ValueIterator<Map.Entry<K, V>> {
        private final RocksIterator iterator;
        private final byte[] prefix;

        public MapIterator(byte[] prefix) {
            this.iterator = RocksMultiMap.this.iterator();
            this.prefix = prefix;
            this.iterator.seek(prefix);
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid() && KeyBuilder.hasPrefix(iterator.key(), prefix);
        }

        @Override
        public Map.Entry<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            byte[] valueBytes = iterator.value();
            iterator.next();

            var keyBytes = KeyBuilder.removePrefix(key, prefix);
            K keyObj = valueSerializer.deserialize(keyBytes, keyType);
            V valueObj = valueSerializer.deserialize(valueBytes, valueType);

            return new AbstractMap.SimpleEntry<>(keyObj, valueObj);
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
