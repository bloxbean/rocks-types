package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.SetMetadata;
import com.bloxbean.rocks.types.collection.util.EmptyIterator;
import com.bloxbean.rocks.types.collection.util.ReverseValueIterator;
import com.bloxbean.rocks.types.collection.util.ValueIterator;
import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.*;

import static com.bloxbean.rocks.types.common.KeyBuilder.bytesToLong;
import static com.bloxbean.rocks.types.common.KeyBuilder.longToBytes;

/**
 * Provides ZSet functionality on top of RocksDB. ZSet is a sorted set where each member is associated with a score.
 * It supports multiple lists under the same name with different namespaces.
 *
 * @param <T>
 */
public class RocksMultiZSet<T> extends BaseDataType<T> {

    public RocksMultiZSet(@NonNull RocksDBConfig rocksDBConfig, String columnFamily,
                          @NonNull String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public RocksMultiZSet(@NonNull RocksDBConfig rocksDBConfig,
                          @NonNull String name, Class<T> valueType) {
        super(rocksDBConfig, null, name, valueType);
    }

    @SneakyThrows
    public void add(String ns, T member, Long score) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, member, score);
    }

    public void addBatch(String ns, WriteBatch writeBatch, Tuple<T, Long>... membersWithScores) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var memberWithScore : membersWithScores) {
            add(ns, writeBatch, metadata, memberWithScore._1, memberWithScore._2);
        }
    }

    private void add(String ns, WriteBatch writeBatch, SetMetadata metadata, T value, Long score) {
        write(writeBatch, getMemberSubKey(metadata, ns, value), valueSerializer.serialize(score));
        write(writeBatch, getScoreSubKey(metadata, ns, value, score), new byte[0]);
    }

    public Optional<Long> getScore(String ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Optional.empty();

        byte[] val = get(getMemberSubKey(metadata.get(), ns, member));
        if (val == null)
            return Optional.empty();
        else
            return Optional.of(valueSerializer.deserialize(val, Long.class));
    }

    @SneakyThrows
    public boolean contains(String ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return false;

        byte[] val = get(getMemberSubKey(metadata.get(), ns, member));
        return val != null;
    }

    @SneakyThrows
    public void remove(String ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;
        WriteBatch writeBatch = new WriteBatch();
        delete(ns, writeBatch, metadata.get(), member);
        db.write(new WriteOptions(), writeBatch);
    }

    @SneakyThrows
    public void removeBatch(String ns, WriteBatch writeBatch, T... member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;
        for (var value : member)
            delete(ns, writeBatch, metadata.get(), value);
    }

    private void delete(String ns, WriteBatch writeBatch, SetMetadata metadata, T member) {
        var score = getScore(ns, member);
        if (score.isPresent()) {
            deleteBatch(writeBatch, getMemberSubKey(metadata, ns, member));
            deleteBatch(writeBatch, getScoreSubKey(metadata, ns, member, score.get()));
        }
    }

    @SneakyThrows
    public Set<T> members(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptySet();

        Set<T> members = new HashSet<>();
        byte[] prefix = getMemberSubKey(metadata.get(), ns, null);
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!KeyBuilder.hasPrefix(key, prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }
                var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefix);
                var parts = KeyBuilder.parts(keyWithoutPrefix);
                T member = valueSerializer.deserialize(parts.get(0), valueType);

                members.add(member);
            }
        }
        return members;
    }

    @SneakyThrows
    public Set<Tuple<T, Long>> membersWithScores(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptySet();

        Set<Tuple<T, Long>> members = new HashSet<>();
        byte[] prefix = getMemberSubKey(metadata.get(), ns, null);
        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!KeyBuilder.hasPrefix(key, prefix)) {
                    break; // Break if the key no longer starts with the prefix
                }

                var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefix);
                var parts = KeyBuilder.parts(keyWithoutPrefix);
                T member = valueSerializer.deserialize(parts.get(0), valueType);
                var score = valueSerializer.deserialize(iterator.value(), Long.class);
                members.add(new Tuple<>(member, score));
            }
        }
        return members;
    }

    public List<Tuple<T, Long>> membersInRange(String ns, long beginningScore, long endScore) {
        //Iterate over the range of scores
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return Collections.emptyList();
        List<Tuple<T, Long>> members = new ArrayList<>();

        byte[] prefixWithoutScore = getScoreSubKeyPrefix(metadata.get(), ns);
        byte[] prefix = KeyBuilder.appendToKey(prefixWithoutScore, longToBytes(beginningScore));

        try (RocksIterator iterator = iterator()) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!KeyBuilder.hasPrefix(key, prefixWithoutScore)) {
                    break; // Break if the key no longer starts with the prefix
                }

                var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefixWithoutScore);
                var parts = KeyBuilder.parts(keyWithoutPrefix);
                long score = bytesToLong(parts.get(0));
                T member = valueSerializer.deserialize(parts.get(1), valueType);
                if (score > endScore)
                    break;

                members.add(new Tuple<>(member, score));
            }
        }

        return members;
    }

    public ValueIterator<Tuple<T, Long>> membersWithScoresIterator(String ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefix = getMemberSubKey(metadata.get(), ns, null);
        return new ZSetMembersIterator<>(iterator(), prefix, valueSerializer, valueType);
    }

    public ValueIterator<Tuple<T, Long>> membersInRangeIterator(String ns, long beginningScore, long endScore) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefixWithoutScore = getScoreSubKeyPrefix(metadata.get(), ns);
        return new ZSetRangeIterator(iterator(), prefixWithoutScore, beginningScore, endScore,
                valueSerializer, valueType);
    }

    public ReverseValueIterator<Tuple<T, Long>> membersInRangeReverseIterator(String ns, long startScore, long endScore) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefixWithoutScore = getScoreSubKeyPrefix(metadata.get(), ns);
        return new ZSetReverseRangeIterator(iterator(), prefixWithoutScore, startScore, endScore,
                valueSerializer, valueType);
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
            return new KeyBuilder(name, ns)
                    .build();
        else
            return new KeyBuilder(name)
                    .build();
    }

    private byte[] getMemberSubKey(SetMetadata metadata, String ns, T member) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(metadata.getVersion())
                    .append("members")
                    .append(member != null ? valueSerializer.serialize(member) : null)
                    .build();
        else
            return new KeyBuilder(name)
                    .append(metadata.getVersion())
                    .append("members")
                    .append(member != null ? valueSerializer.serialize(member) : null)
                    .build();
    }

    public byte[] getScoreSubKey(SetMetadata metadata, String ns, T member, long score) {
        byte[] scorePrefix = getScoreSubKeyPrefix(metadata, ns);
        var key = KeyBuilder.appendToKey(scorePrefix, longToBytes(score), valueSerializer.serialize(member));

        return key;
    }

    //This is without score
    private byte[] getScoreSubKeyPrefix(SetMetadata metadata, String ns) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(metadata.getVersion())
                    .append("scores")
                    .build();
        else
            return new KeyBuilder(name)
                    .append(metadata.getVersion())
                    .append("scores")
                    .build();
    }

    private class ZSetRangeIterator<T> implements ValueIterator<Tuple<T, Long>> {
        private final RocksIterator iterator;
        private final byte[] prefixWithoutScore;
        private final long endScore;
        private final Serializer valueSerializer;
        private final Class<T> valueType;

        public ZSetRangeIterator(@NonNull RocksIterator iterator,
                                 @NonNull byte[] prefixWithoutScore,
                                 long beginningScore,
                                 long endScore,
                                 @NonNull Serializer valueSerializer,
                                 @NonNull Class<T> valueType) {
            this.iterator = iterator;
            this.prefixWithoutScore = prefixWithoutScore;
            this.endScore = endScore;
            this.valueSerializer = valueSerializer;
            this.valueType = valueType;
            byte[] prefix = KeyBuilder.appendToKey(prefixWithoutScore, longToBytes(beginningScore));
            this.iterator.seek(prefix);
        }

        @Override
        public boolean hasNext() {
            if (!iterator.isValid() || !KeyBuilder.hasPrefix(iterator.key(), prefixWithoutScore)) {
                return false;
            }
            var keyWithoutPrefix = KeyBuilder.removePrefix(iterator.key(), prefixWithoutScore);
            var parts = KeyBuilder.parts(keyWithoutPrefix);
            long score = bytesToLong(parts.get(0));
            return score <= endScore;
        }

        @Override
        public Tuple<T, Long> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            iterator.next();

            var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefixWithoutScore);
            var parts = KeyBuilder.parts(keyWithoutPrefix);
            long score = bytesToLong(parts.get(0));
            T member = valueSerializer.deserialize(parts.get(1), valueType);

            return new Tuple<>(member, score);
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

    private class ZSetMembersIterator<T> implements ValueIterator<Tuple<T, Long>> {
        private final RocksIterator iterator;
        private final byte[] prefix;
        private final Serializer valueSerializer;
        private final Class<T> valueType;

        public ZSetMembersIterator(@NonNull RocksIterator rocksIterator,
                                   @NonNull byte[] prefix,
                                   @NonNull Serializer valueSerializer,
                                   @NonNull Class<T> valueType) {
            this.iterator = rocksIterator;
            this.prefix = prefix;
            this.valueSerializer = valueSerializer;
            this.valueType = valueType;
            this.iterator.seek(prefix);
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid() && KeyBuilder.hasPrefix(iterator.key(), prefix);
        }

        @Override
        public Tuple<T, Long> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            byte[] value = iterator.value();
            iterator.next();

            var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefix);
            var parts = KeyBuilder.parts(keyWithoutPrefix);
            T member = valueSerializer.deserialize(parts.get(0), valueType);
            Long score = valueSerializer.deserialize(value, Long.class);

            return new Tuple<>(member, score);
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

    private class ZSetReverseRangeIterator<T> implements ReverseValueIterator<Tuple<T, Long>> {
        private final RocksIterator iterator;
        private final byte[] prefixWithoutScore;
        private final long startScore; // Score to start iterating from, in reverse
        private final long endScore;
        private final Serializer valueSerializer;
        private final Class<T> valueType;

        public ZSetReverseRangeIterator(@NonNull RocksIterator iterator,
                                        @NonNull byte[] prefixWithoutScore,
                                        long startScore, // Score to start iterating from, in reverse
                                        long endScore,
                                        @NonNull Serializer valueSerializer,
                                        @NonNull Class<T> valueType) {
            this.iterator = iterator;
            this.prefixWithoutScore = prefixWithoutScore;
            this.startScore = startScore;
            this.endScore = endScore;
            this.valueSerializer = valueSerializer;
            this.valueType = valueType;

            // Seek to the starting position for reverse iteration
            //to include start score in the result
            byte[] startPrefix = KeyBuilder.appendToKey(prefixWithoutScore, longToBytes(this.startScore + 1));
            this.iterator.seekForPrev(startPrefix);
        }

        @Override
        public boolean hasPrev() {
            if (!iterator.isValid() || !KeyBuilder.hasPrefix(iterator.key(), prefixWithoutScore)) {
                return false;
            }
            var keyWithoutPrefix = KeyBuilder.removePrefix(iterator.key(), prefixWithoutScore);
            var parts = KeyBuilder.parts(keyWithoutPrefix);
            long score = bytesToLong(parts.get(0));
            return score <= startScore && score >= endScore;
        }

        @Override
        public Tuple<T, Long> prev() {
            if (!hasPrev()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            iterator.prev(); // Move to the previous item

            var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefixWithoutScore);
            var parts = KeyBuilder.parts(keyWithoutPrefix);
            long score = bytesToLong(parts.get(0));
            T member = valueSerializer.deserialize(parts.get(1), valueType);

            return new Tuple<>(member, score);
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

}

