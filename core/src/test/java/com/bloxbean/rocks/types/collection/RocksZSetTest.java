package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.junit.jupiter.api.Assertions.*;

class RocksZSetTest extends RocksBaseTest {

    @Test
    void add() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        var members = rocksZSet.members();
        assertEquals(3, members.size());
        assertTrue(members.contains("one"));
        assertTrue(members.contains("two"));
        assertTrue(members.contains("three"));
    }

    @Test
    void addBatch() throws Exception {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksZSet.addBatch(writeBatch, new Tuple<>("one", 1L), new Tuple<>("two", 2L), new Tuple<>("three", 3L));
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        var members = rocksZSet.members();
        assertEquals(3, members.size());
        assertTrue(members.contains("one"));
        assertTrue(members.contains("two"));
        assertTrue(members.contains("three"));
    }

    @Test
    void getScore() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        var score = rocksZSet.getScore("one");
        assertTrue(score.isPresent());
        assertEquals(1L, score.get());
    }

    @Test
    void contains() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        assertTrue(rocksZSet.contains("one"));
        assertFalse(rocksZSet.contains("four"));
        assertTrue(rocksZSet.contains("two"));
    }

    @Test
    void remove() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        rocksZSet.remove("one");
        assertFalse(rocksZSet.contains("one"));
        assertTrue(rocksZSet.contains("two"));
        assertTrue(rocksZSet.contains("three"));
    }

    @Test
    void removeBatch() throws Exception {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        WriteBatch writeBatch = new WriteBatch();
        rocksZSet.removeBatch(writeBatch, "one", "two");
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertFalse(rocksZSet.contains("one"));
        assertFalse(rocksZSet.contains("two"));
        assertTrue(rocksZSet.contains("three"));
    }

    @Test
    void members() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        var members = rocksZSet.members();
        assertEquals(3, members.size());
        assertTrue(members.contains("one"));
        assertTrue(members.contains("two"));
        assertTrue(members.contains("three"));
    }

    @Test
    void membersWithScores() {
        RocksZSet rocksZSet = new RocksZSet(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);

        var members = rocksZSet.membersWithScores();
        assertEquals(3, members.size());
        assertTrue(members.contains(new Tuple<>("one", 1L)));
        assertTrue(members.contains(new Tuple<>("two", 2L)));
        assertTrue(members.contains(new Tuple<>("three", 3L)));
    }

    @Test
    void membersInRange() {
        var rocksZSet = new RocksZSet<>(rocksDBConfig, "zset1", String.class);
        rocksZSet.add("one", 1L);
        rocksZSet.add("two", 2L);
        rocksZSet.add("three", 3L);
        rocksZSet.add("four", 4L);
        rocksZSet.add("ten", 10L);
        rocksZSet.add("eight", 8L);
        rocksZSet.add("seven", 7L);
        rocksZSet.add("twentyone", 21L);
        rocksZSet.add("twenty", 20L);
        rocksZSet.add("twentytwo", 22L);
        rocksZSet.add("thirty", 30L);

        var members = rocksZSet.membersInRange(1, 10).stream().map(t -> t._1).toList();
        assertEquals(7, members.size());
        assertTrue(members.contains("one"));
        assertTrue(members.contains("two"));
        assertTrue(members.contains("three"));
        assertTrue(members.contains("four"));
        assertTrue(members.contains("seven"));
        assertTrue(members.contains("eight"));
        assertTrue(members.contains("ten"));
    }
}
