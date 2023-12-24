package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.common.Tuple;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.bloxbean.rocks.types.common.KeyBuilder.bytesToLong;
import static com.bloxbean.rocks.types.common.KeyBuilder.longToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RocksMultiZSetTest extends RocksBaseTest {
    byte[] ns = "ns1".getBytes();

    @Override
    public String getColumnFamilies() {
        return "zset1";
    }

    @Test
    void add_members() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var members = rocksDBZSet.members(ns);
        assertEquals(11, members.size());
        assertThat(members).contains("one", "two", "three", "four", "ten", "eight", "seven", "twentyone", "twenty", "twentytwo", "thirty");
    }

    @Test
    void add_members_defaultCF() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(null, "two", 2L);
        rocksDBZSet.add(null, "one", 1L);
        rocksDBZSet.add(null, "three", 3L);
        rocksDBZSet.add(null, "four", 4L);
        rocksDBZSet.add(null, "ten", 10L);
        rocksDBZSet.add(null, "eight", 8L);
        rocksDBZSet.add(null, "seven", 7L);
        rocksDBZSet.add(null, "twentyone", 21L);
        rocksDBZSet.add(null, "twenty", 20L);
        rocksDBZSet.add(null, "twentytwo", 22L);
        rocksDBZSet.add(null, "thirty", 30L);

        var members = rocksDBZSet.members(null);
        assertEquals(11, members.size());
        assertThat(members).contains("one", "two", "three", "four", "ten", "eight", "seven", "twentyone", "twenty", "twentytwo", "thirty");
    }

    @Test
    void addBatch_members() throws Exception {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBZSet.addBatch(ns, writeBatch, new Tuple<>("two", 2L), new Tuple<>("one", 1L), new Tuple<>("three", 3L), new Tuple<>("four", 4L),
                new Tuple<>("ten", 10L), new Tuple<>("eight", 8L), new Tuple<>("seven", 7L), new Tuple<>("twentyone", 21L),
                new Tuple<>("twenty", 20L), new Tuple<>("twentytwo", 22L), new Tuple<>("thirty", 30L));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        var members = rocksDBZSet.members(ns);
        assertEquals(11, members.size());
        assertThat(members).contains("one", "two", "three", "four", "ten", "eight", "seven", "twentyone", "twenty", "twentytwo", "thirty");
    }

    @Test
    void addBatch_getScore() throws Exception {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBZSet.addBatch(ns, writeBatch, new Tuple<>("two", 2L), new Tuple<>("one", 1L), new Tuple<>("three", 3L), new Tuple<>("four", 4L),
                new Tuple<>("ten", 10L), new Tuple<>("eight", 8L), new Tuple<>("seven", 7L), new Tuple<>("twentyone", 21L),
                new Tuple<>("twenty", 20L), new Tuple<>("twentytwo", 22L), new Tuple<>("thirty", 30L));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        var score = rocksDBZSet.getScore(ns, "twentytwo");
        var score2 = rocksDBZSet.getScore(ns, "eighty");
        assertThat(score).isEqualTo(Optional.of(22L));
        assertThat(score2).isEmpty();
    }

    @Test
    void add_members_withScores() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        Set<Tuple<String, Long>> membersWithScores = rocksDBZSet.membersWithScores(ns);
        System.out.println(membersWithScores);

        assertThat(membersWithScores).hasSize(11);
        assertThat(membersWithScores.stream().map(m -> m._1).collect(Collectors.toList())).contains("one", "two", "three", "four", "ten", "eight", "seven", "twentyone", "twenty", "twentytwo", "thirty");
        assertThat(membersWithScores.stream().map(m -> m._2).collect(Collectors.toList())).contains(1L, 2L, 3L, 4L, 10L, 8L, 7L, 21L, 20L, 22L, 30L);
    }

    @Test
    void add_members_inrange() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var membersWithScore = rocksDBZSet.membersInRange(ns, 7L, 21L);
        membersWithScore.forEach(System.out::println);
        assertThat(membersWithScore).hasSize(5);
        assertThat(membersWithScore.get(0)).isEqualTo(new Tuple<>("seven", 7L));
        assertThat(membersWithScore.get(1)).isEqualTo(new Tuple<>("eight", 8L));
        assertThat(membersWithScore.get(2)).isEqualTo(new Tuple<>("ten", 10L));
        assertThat(membersWithScore.get(3)).isEqualTo(new Tuple<>("twenty", 20L));
        assertThat(membersWithScore.get(4)).isEqualTo(new Tuple<>("twentyone", 21L));
    }

    @Test
    void add_contains() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet<String>(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var present = rocksDBZSet.contains(ns, "seven");
        var notPresent = rocksDBZSet.contains(ns, "fourty");
        assertTrue(present);
        assertFalse(notPresent);
    }

    @Test
    void add_remove() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        rocksDBZSet.remove(ns, "seven");
        rocksDBZSet.remove(ns, "fourty");
        rocksDBZSet.remove(ns, "one");

        var present1 = rocksDBZSet.contains(ns, "seven");
        var present2 = rocksDBZSet.contains(ns, "fourty");
        var present3 = rocksDBZSet.contains(ns, "one");
        var present4 = rocksDBZSet.contains(ns, "two");
        var present5 = rocksDBZSet.contains(ns, "three");

        assertFalse(present1);
        assertFalse(present2);
        assertFalse(present3);
        assertTrue(present4);
        assertTrue(present5);
    }

    @Test
    void add_removeBatch() throws Exception {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        WriteBatch writeBatch = new WriteBatch();
        rocksDBZSet.removeBatch(ns, writeBatch, "seven", "fourty", "one");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        var present1 = rocksDBZSet.contains(ns, "seven");
        var present2 = rocksDBZSet.contains(ns, "fourty");
        var present3 = rocksDBZSet.contains(ns, "one");
        var present4 = rocksDBZSet.contains(ns, "two");
        var present5 = rocksDBZSet.contains(ns, "three");

        assertFalse(present1);
        assertFalse(present2);
        assertFalse(present3);
        assertTrue(present4);
        assertTrue(present5);
    }

    @Test
    void add_members_inrange_iterable() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var iterator = rocksDBZSet.membersInRangeIterator(ns, 7L, 21L);

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
    void add_members_inrange_reverse_iterable() {
        RocksMultiZSet rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var iterator = rocksDBZSet.membersInRangeReverseIterator(ns, 7L, 2L);

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

    @Test
    void add_membersWithScores_iterable() {
        RocksMultiZSet<String> rocksDBZSet = new RocksMultiZSet(rocksDBConfig, "zset1", "names", String.class);
        rocksDBZSet.add(ns, "two", 2L);
        rocksDBZSet.add(ns, "one", 1L);
        rocksDBZSet.add(ns, "three", 3L);
        rocksDBZSet.add(ns, "four", 4L);
        rocksDBZSet.add(ns, "ten", 10L);
        rocksDBZSet.add(ns, "eight", 8L);
        rocksDBZSet.add(ns, "seven", 7L);
        rocksDBZSet.add(ns, "twentyone", 21L);
        rocksDBZSet.add(ns, "twenty", 20L);
        rocksDBZSet.add(ns, "twentytwo", 22L);
        rocksDBZSet.add(ns, "thirty", 30L);

        var iterator = rocksDBZSet.membersWithScoresIterator(ns);

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
    void membersInRangeReverse() {
        long l = 10636858L;
        byte[] ls = longToBytes(l);
        System.out.println(new String(ls));
        System.out.println(bytesToLong(ls));
        String address = "addr_test1vzpwq95z3xyum8vqndgdd9mdnmafh3djcxnc6jemlgdmswcve6tkw";
        BigInteger amount = BigInteger.valueOf(10638239);

        byte[] ns = (address + "_" + "lovelace").getBytes(StandardCharsets.UTF_8);
        RocksMultiZSet<byte[]> zset = new RocksMultiZSet<>(rocksDBConfig, "zset", "test", byte[].class);

        zset.add(ns, getAddressBalanceKey(address , "lovelace", 10634820), 10634820L);
        zset.add(ns, getAddressBalanceKey(address , "lovelace", 10634828), 10634828L);
        zset.add(ns, getAddressBalanceKey(address , "lovelace", 10636858), 10636858L);

        var iterator = zset.membersInRangeReverseIterator(ns, 10638239, 0);
        var members = new ArrayList<Tuple<byte[], Long>>();
        while (iterator.hasPrev()) {
            members.add(iterator.prev());
        }

        assertThat(members).hasSize(3);
    }

    private static byte[] getAddressBalanceKey(String address, String lovelace, int i) {
        return (address + "_" + lovelace + "_" + i).getBytes();
    }
}
