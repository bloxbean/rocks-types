package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.SetMetadata;
import com.bloxbean.rocks.types.collection.util.EmptyIterator;
import com.bloxbean.rocks.types.collection.util.ReverseValueIterator;
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
                          @NonNull String name, Class<T> memberType) {
        super(rocksDBConfig, columnFamily, name, memberType);
    }

    public RocksMultiZSet(@NonNull RocksDBConfig rocksDBConfig,
                          @NonNull String name, Class<T> memberType) {
        super(rocksDBConfig, null, name, memberType);
    }

    @SneakyThrows
    public void add(byte[] ns, T member, Long score) {
        var metadata = createMetadata(ns).orElseThrow();
        add(ns, null, metadata, member, score);
    }

    public void addBatch(byte[] ns, WriteBatch writeBatch, Tuple<T, Long>... membersWithScores) {
        var metadata = createMetadata(ns).orElseThrow();
        for (var memberWithScore : membersWithScores) {
            add(ns, writeBatch, metadata, memberWithScore._1, memberWithScore._2);
        }
    }

    private void add(byte[] ns, WriteBatch writeBatch, SetMetadata metadata, T member, Long score) {
        write(writeBatch, getMemberSubKey(metadata, ns, member), valueSerializer.serialize(score));
        write(writeBatch, getScoreSubKey(metadata, ns, member, score), new byte[0]);
    }

    public Optional<Long> getScore(byte[] ns, T member) {
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
    public boolean contains(byte[] ns, T member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return false;

        byte[] val = get(getMemberSubKey(metadata.get(), ns, member));
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
    public void removeBatch(byte[] ns, WriteBatch writeBatch, T... member) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty())
            return;
        for (var value : member)
            delete(ns, writeBatch, metadata.get(), value);
    }

    private void delete(byte[] ns, WriteBatch writeBatch, SetMetadata metadata, T member) {
        var score = getScore(ns, member);
        if (score.isPresent()) {
            deleteBatch(writeBatch, getMemberSubKey(metadata, ns, member));
            deleteBatch(writeBatch, getScoreSubKey(metadata, ns, member, score.get()));
        }
    }

    @SneakyThrows
    public Set<T> members(byte[] ns) {
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

                T member = getMemberFromMemberCompositeSubKey(key);

                members.add(member);
            }
        }
        return members;
    }

    @SneakyThrows
    public Set<Tuple<T, Long>> membersWithScores(byte[] ns) {
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

                T member = getMemberFromMemberCompositeSubKey(key);
                var score = valueSerializer.deserialize(iterator.value(), Long.class);
                members.add(new Tuple<>(member, score));
            }
        }
        return members;
    }

    public List<Tuple<T, Long>> membersInRange(byte[] ns, long beginningScore, long endScore) {
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

                Tuple<T, Long> memberAndScore = getMemberAndScoreFromScoreCompositeSubKey(key);
                if (memberAndScore._2 > endScore)
                    break;

                members.add(memberAndScore);
            }
        }

        return members;
    }

    public ValueIterator<Tuple<T, Long>> membersWithScoresIterator(byte[] ns) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefix = getMemberSubKey(metadata.get(), ns, null);
        return new ZSetMembersIterator<>(iterator(), prefix);
    }

    public ValueIterator<Tuple<T, Long>> membersInRangeIterator(byte[] ns, long beginningScore, long endScore) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefixWithoutScore = getScoreSubKeyPrefix(metadata.get(), ns);
        return new ZSetRangeIterator(iterator(), prefixWithoutScore, beginningScore, endScore
        );
    }

    public ReverseValueIterator<Tuple<T, Long>> membersInRangeReverseIterator(byte[] ns, long startScore, long endScore) {
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            return new EmptyIterator<>();
        }
        byte[] prefixWithoutScore = getScoreSubKeyPrefix(metadata.get(), ns);
        return new ZSetReverseRangeIterator(iterator(), prefixWithoutScore, startScore, endScore
        );
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

    private byte[] getMemberSubKey(SetMetadata metadata, byte[] ns, T member) {
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

    public byte[] getScoreSubKey(SetMetadata metadata, byte[] ns, T member, long score) {
        byte[] scorePrefix = getScoreSubKeyPrefix(metadata, ns);
        var key = KeyBuilder.appendToKey(scorePrefix, longToBytes(score), valueSerializer.serialize(member));

        return key;
    }

    //This is without score
    private byte[] getScoreSubKeyPrefix(SetMetadata metadata, byte[] ns) {
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

    private <T> T getMemberFromMemberCompositeSubKey(byte[] key) {
        var parts = KeyBuilder.decodeCompositeKey(key);
        return (T) valueSerializer.deserialize(parts.get(parts.size() - 1), valueType);
    }

    private Tuple<T, Long> getMemberAndScoreFromScoreCompositeSubKey(byte[] key) {
        var parts = KeyBuilder.decodeCompositeKey(key);
        long score = bytesToLong(parts.get(parts.size() - 2));
        T member = valueSerializer.deserialize(parts.get(parts.size() - 1), valueType);
        return new Tuple<>(member, score);
    }

    private class ZSetRangeIterator<T> implements ValueIterator<Tuple<T, Long>> {
        private final RocksIterator iterator;
        private final byte[] prefixWithoutScore;
        private final long endScore;

        public ZSetRangeIterator(@NonNull RocksIterator iterator,
                                 @NonNull byte[] prefixWithoutScore,
                                 long beginningScore,
                                 long endScore) {
            this.iterator = iterator;
            this.prefixWithoutScore = prefixWithoutScore;
            this.endScore = endScore;
            byte[] prefix = KeyBuilder.appendToKey(prefixWithoutScore, longToBytes(beginningScore));
            this.iterator.seek(prefix);
        }

        @Override
        public boolean hasNext() {
            if (!iterator.isValid() || !KeyBuilder.hasPrefix(iterator.key(), prefixWithoutScore)) {
                return false;
            }

            long score = getMemberAndScoreFromScoreCompositeSubKey(iterator.key())._2;
            return score <= endScore;
        }

        @Override
        public Tuple<T, Long> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            iterator.next();

            Tuple<T, Long> memberAndScore = (Tuple<T, Long>) getMemberAndScoreFromScoreCompositeSubKey(key);
            return memberAndScore;
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

        public ZSetMembersIterator(@NonNull RocksIterator rocksIterator,
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
        public Tuple<T, Long> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            byte[] value = iterator.value();
            iterator.next();

            T member = getMemberFromMemberCompositeSubKey(key);
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

        public ZSetReverseRangeIterator(@NonNull RocksIterator iterator,
                                        @NonNull byte[] prefixWithoutScore,
                                        long startScore, // Score to start iterating from, in reverse
                                        long endScore) {
            this.iterator = iterator;
            this.prefixWithoutScore = prefixWithoutScore;
            this.startScore = startScore;
            this.endScore = endScore;

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

            long score = getMemberAndScoreFromScoreCompositeSubKey(iterator.key())._2;
            return score <= startScore && score >= endScore;
        }

        @Override
        public Tuple<T, Long> prev() {
            if (!hasPrev()) {
                throw new NoSuchElementException();
            }
            byte[] key = iterator.key();
            iterator.prev(); // Move to the previous item

            Tuple<T, Long> memberAndScore = (Tuple<T, Long>) getMemberAndScoreFromScoreCompositeSubKey(key);

            return memberAndScore;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }

}

