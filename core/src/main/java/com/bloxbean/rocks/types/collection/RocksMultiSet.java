package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.SetMetadata;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class RocksMultiSet extends BaseDataType {

    public RocksMultiSet(RocksDBConfig rocksDBConfig, String columnFamily, String name) {
        super(rocksDBConfig, columnFamily, name, null);
    }

    public RocksMultiSet(RocksDBConfig rocksDBConfig, String name) {
        super(rocksDBConfig, null, name, null);
    }

    @SneakyThrows
    public void add(String ns, String value) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, value);
    }

    public void addBatch(String ns, WriteBatch writeBatch, String... value) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var val : value) {
            add(ns, writeBatch, metadata, val);
        }
    }

    private void add(String ns, WriteBatch writeBatch, SetMetadata metadata, String value) {
        write(writeBatch, keySerializer.serialize(getSubKey(metadata, ns, value)), new byte[0]);
    }

    @SneakyThrows
    public boolean contains(String ns, String value) {
        var metadata = getMetadata(ns).orElseThrow();
        byte[] val = get(keySerializer.serialize(getSubKey(metadata, ns, value)));
        return val != null;
    }

    @SneakyThrows
    public void remove(String ns, String value) {
        var metadata = getMetadata(ns).orElseThrow();
        WriteBatch writeBatch = new WriteBatch();
        delete(ns, writeBatch, metadata, value);
        db.write(new WriteOptions(), writeBatch);
    }

    @SneakyThrows
    public void removeBatch(String ns, WriteBatch writeBatch, String... values) {
        var metadata = getMetadata(ns).orElseThrow();
        for (var value : values)
            delete(ns, writeBatch, metadata, value);
    }

    private void delete(String ns, WriteBatch writeBatch, SetMetadata metadata, String value) {
        deleteBatch(writeBatch, keySerializer.serialize(getSubKey(metadata, ns, value)));
    }

    @SneakyThrows
    public Set<String> members(String ns) {
        var metadata = getMetadata(ns).orElseThrow();
        Set<String> members = new HashSet<>();
        String prefix = getSubKey(metadata, ns, "");
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(keySerializer.serialize(prefix)); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                if (!key.startsWith(prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }
                String member = key.substring(prefix.length());
                members.add(member);
            }
        }
        return members;
    }

    @SneakyThrows
    protected Optional<SetMetadata> getMetadata(String ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, SetMetadata.class));
        }
    }

    @Override
    protected Optional<SetMetadata> createMetadata(String ns) {
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

    protected byte[] getMetadataKey(String ns) {
        if (ns != null)
            return keySerializer.serialize(name + PREFIX + ns);
        else
            return keySerializer.serialize(name);
    }

    private String getSubKey(SetMetadata metadata, String ns, String value) {
        if (ns != null)
            return name + PREFIX + ns + PREFIX + metadata.getVersion() + PREFIX + value;
        else
            return name + PREFIX + metadata.getVersion() + PREFIX + value;
    }
}

