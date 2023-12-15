package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.rocksdb.WriteBatch;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Provides ZSet functionality on top of RocksDB. ZSet is a sorted set where each member is associated with a score.
 * @param <T>
 */
public class RocksZSet<T> extends RocksMultiZSet<T> {

    public RocksZSet(@NonNull RocksDBConfig rocksDBConfig, @NonNull String name, Class<T> valueType) {
        super(rocksDBConfig, name, valueType);
    }

    public RocksZSet(@NonNull RocksDBConfig rocksDBConfig, String columnFamily, @NonNull String name, Class<T> valueType) {
        super(rocksDBConfig, columnFamily, name, valueType);
    }

    public void add(T member, Long score) {
        add(null, member, score);
    }

    public void addBatch(WriteBatch writeBatch, Tuple<T, Long>... membersWithScores) {
       addBatch(null, writeBatch, membersWithScores);
    }

    public Optional<Long> getScore(T member) {
        return getScore(null, member);
    }

    @SneakyThrows
    public boolean contains(T member) {
       return contains(null, member);
    }

    @SneakyThrows
    public void remove(T member) {
        remove(null, member);
    }

    @SneakyThrows
    public void removeBatch(WriteBatch writeBatch, T... members) {
        removeBatch(null, writeBatch, members);
    }

    @SneakyThrows
    public Set<T> members() {
       return members(null);
    }

    @SneakyThrows
    public Set<Tuple<T, Long>> membersWithScores() {
        return membersWithScores(null);
    }

    public List<Tuple<T, Long>> membersInRange(long beginningScore, long endScore) {
        return membersInRange(null, beginningScore, endScore);
    }

}

