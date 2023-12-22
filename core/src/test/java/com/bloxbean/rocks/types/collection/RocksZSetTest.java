package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
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

    @Test
    void add_members_inrange_iterable() {
        RocksZSet rocksDBZSet = new RocksZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add("two", 2L);
        rocksDBZSet.add("one", 1L);
        rocksDBZSet.add("three", 3L);
        rocksDBZSet.add("four", 4L);
        rocksDBZSet.add("ten", 10L);
        rocksDBZSet.add("eight", 8L);
        rocksDBZSet.add("seven", 7L);
        rocksDBZSet.add("twentyone", 21L);
        rocksDBZSet.add("twenty", 20L);
        rocksDBZSet.add("twentytwo", 22L);
        rocksDBZSet.add("thirty", 30L);

        var iterator = rocksDBZSet.membersInRangeIterable(7L, 21L);

        var membersWithScore = new ArrayList<>();
        while (iterator.hasNext()) {
            membersWithScore.add(iterator.next());
        }

        membersWithScore.forEach(System.out::println);
        assertThat(membersWithScore).hasSize(5);
        assertThat(membersWithScore.get(0)).isEqualTo(new Tuple<>("seven", 7L));
        assertThat(membersWithScore.get(1)).isEqualTo(new Tuple<>("eight", 8L));
        assertThat(membersWithScore.get(2)).isEqualTo(new Tuple<>("ten", 10L));
        assertThat(membersWithScore.get(3)).isEqualTo(new Tuple<>("twenty", 20L));
        assertThat(membersWithScore.get(4)).isEqualTo(new Tuple<>("twentyone", 21L));
    }

    @Test
    void add_membersWithScores_iterable() {
        RocksZSet<String> rocksDBZSet = new RocksZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add("two", 2L);
        rocksDBZSet.add("one", 1L);
        rocksDBZSet.add("three", 3L);
        rocksDBZSet.add("four", 4L);
        rocksDBZSet.add("ten", 10L);
        rocksDBZSet.add("eight", 8L);
        rocksDBZSet.add("seven", 7L);
        rocksDBZSet.add("twentyone", 21L);
        rocksDBZSet.add("twenty", 20L);
        rocksDBZSet.add("twentytwo", 22L);
        rocksDBZSet.add("thirty", 30L);

        var iterator = rocksDBZSet.membersWithScoresIterable();

        var membersWithScore = new ArrayList<Tuple<String, Long>>();
        while (iterator.hasNext()) {
            membersWithScore.add(iterator.next());
        }

        membersWithScore.forEach(System.out::println);
        assertThat(membersWithScore).hasSize(11);
        assertThat(membersWithScore.stream().map(m -> m._1)
                .collect(Collectors.toList()))
                .contains("one", "two", "three", "four", "ten", "eight", "seven", "twentyone", "twenty", "twentytwo", "thirty");
    }

    @Test
    void add_members_inrange_reverse_iterable() {
        RocksZSet rocksDBZSet = new RocksZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add("two", 2L);
        rocksDBZSet.add("one", 1L);
        rocksDBZSet.add("three", 3L);
        rocksDBZSet.add("four", 4L);
        rocksDBZSet.add("ten", 10L);
        rocksDBZSet.add("eight", 8L);
        rocksDBZSet.add("seven", 7L);
        rocksDBZSet.add("twentyone", 21L);
        rocksDBZSet.add("twenty", 20L);
        rocksDBZSet.add("twentytwo", 22L);
        rocksDBZSet.add("thirty", 30L);

        var iterator = rocksDBZSet.membersInRangeReverseIterator(7L, 2L);

        var membersWithScore = new ArrayList<>();
        while (iterator.hasPrev()) {
            membersWithScore.add(iterator.prev());
        }

        membersWithScore.forEach(System.out::println);
        assertThat(membersWithScore).hasSize(4);
        assertThat(membersWithScore.get(0)).isEqualTo(new Tuple<>("seven", 7L));
        assertThat(membersWithScore.get(1)).isEqualTo(new Tuple<>("four", 4L));
        assertThat(membersWithScore.get(2)).isEqualTo(new Tuple<>("three", 3L));
        assertThat(membersWithScore.get(3)).isEqualTo(new Tuple<>("two", 2L));
    }
}
