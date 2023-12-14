package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.SetMetadata;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class RocksMultiZSet extends BaseDataType {

    public RocksMultiZSet(RocksDBConfig rocksDBConfig, String columnFamily, String name) {
        super(rocksDBConfig, columnFamily, name, null);
    }

    public RocksMultiZSet(RocksDBConfig rocksDBConfig, String name) {
        super(rocksDBConfig, null, name, null);
    }

    @SneakyThrows
    public void add(String ns, String member, Long score) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, member, score);
    }

    public void addBatch(String ns, WriteBatch writeBatch, Tuple<String, Long>... membersWithScores) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var memberWithScore : membersWithScores) {
            add(ns, writeBatch, metadata, memberWithScore._1, memberWithScore._2);
        }
    }

    private void add(String ns, WriteBatch writeBatch, SetMetadata metadata, String value, Long score) {
        write(writeBatch, keySerializer.serialize(getMemberSubKey(metadata, ns, value)), valueSerializer.serialize(score));
        write(writeBatch, keySerializer.serialize(getScoreSubKey(metadata, ns, value, score)), new byte[0]);
    }

    public Optional<Long> getScore(String ns, String member) {
        var metadata = getMetadata(ns).orElseThrow();
        byte[] val = get(keySerializer.serialize(getMemberSubKey(metadata, ns, member)));
        if (val == null)
            return Optional.empty();
        else
            return Optional.of(valueSerializer.deserialize(val, Long.class));
    }

    @SneakyThrows
    public boolean contains(String ns, String member) {
        var metadata = getMetadata(ns).orElseThrow();
        byte[] val = get(keySerializer.serialize(getMemberSubKey(metadata, ns, member)));
        return val != null;
    }

    @SneakyThrows
    public void remove(String ns, String member) {
        var metadata = getMetadata(ns).orElseThrow();
        WriteBatch writeBatch = new WriteBatch();
        delete(ns, writeBatch, metadata, member);
        db.write(new WriteOptions(), writeBatch);
    }

    @SneakyThrows
    public void removeBatch(String ns, WriteBatch writeBatch, String... member) {
        var metadata = getMetadata(ns).orElseThrow();
        for (var value : member)
            delete(ns, writeBatch, metadata, value);
    }

    private void delete(String ns, WriteBatch writeBatch, SetMetadata metadata, String member) {
        var score = getScore(ns, member);
        if (score.isPresent()) {
            deleteBatch(writeBatch, keySerializer.serialize(getMemberSubKey(metadata, ns, member)));
            deleteBatch(writeBatch, keySerializer.serialize(getScoreSubKey(metadata, ns, member, score.get())));
        }
    }

    @SneakyThrows
    public Set<String> members(String ns) {
        var metadata = getMetadata(ns).orElseThrow();
        Set<String> members = new HashSet<>();
        String prefix = getMemberSubKey(metadata, ns, "");
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
    public Set<Tuple<String, Long>> membersWithScores(String ns) {
        var metadata = getMetadata(ns).orElseThrow();
        Set<Tuple<String, Long>> members = new HashSet<>();
        String prefix = getMemberSubKey(metadata, ns, "");
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(keySerializer.serialize(prefix)); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                if (!key.startsWith(prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }
                String member = key.substring(prefix.length());
                getScore(ns, member).ifPresent(score -> members.add(new Tuple<>(member, score)));
            }
        }
        return members;
    }

//    public Set<String> membersInRange(String ns, long beginningScore, long endScore) {
//        //Iterate over the range of scores
//        var metadata = getMetadata(ns).orElseThrow();
//        Set<String> members = new HashSet<>();
//        String prefix = getScoreSubKey(metadata, ns, "", beginningScore);
//
//        try (RocksIterator iterator = iterator()) {
//            for (iterator.seek(keySerializer.serialize(prefix)); iterator.isValid(); iterator.next()) {
//                String key = new String(iterator.key());
//                if (!key.startsWith(prefix)) {
//                    break; // Break if the key no longer starts with the prefix
//                }
//                String member = key.substring(prefix.length());
//                members.add(member);
//            }
//        }
//    }


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

    private String getMemberSubKey(SetMetadata metadata, String ns, String member) {
        if (ns != null)
            return name + PREFIX + ns + PREFIX + metadata.getVersion() + PREFIX + member;
        else
            return name + PREFIX + metadata.getVersion() + PREFIX + member;
    }

    private String getScoreSubKey(SetMetadata metadata, String ns, String member, long score) {
        if (ns != null)
            return name + PREFIX + ns + PREFIX + metadata.getVersion() + PREFIX + score + PREFIX + member;
        else
            return name + PREFIX + metadata.getVersion() + PREFIX + score + PREFIX + member;
    }
}

