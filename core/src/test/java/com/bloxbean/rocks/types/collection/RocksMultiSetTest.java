package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RocksMultiSetTest extends RocksBaseTest {
    byte[] ns = "ns1".getBytes();
    byte[] ns2 = "ns2".getBytes();

    @Override
    public String getColumnFamilies() {
        return "list-cf";
    }

    @Test
    void addAndContains() {
        RocksMultiSet rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);
        rocksDBSet.add(ns, "one");
        rocksDBSet.add(ns, "two");
        rocksDBSet.add(ns, "one");
        rocksDBSet.add(ns, "nine");

        rocksDBSet.add(ns2, "aaa");
        rocksDBSet.add(ns2, "bbb");
        rocksDBSet.add(ns2, "ccc");
        rocksDBSet.add(ns2, "ddd");

        assertTrue(rocksDBSet.contains(ns, "one"));
        assertTrue(rocksDBSet.contains(ns, "two"));
        assertFalse(rocksDBSet.contains(ns, "three"));
        assertFalse(rocksDBSet.contains(ns, "four"));
        assertTrue(rocksDBSet.contains(ns, "nine"));
        assertFalse(rocksDBSet.contains(ns, "aaa"));
        assertFalse(rocksDBSet.contains(ns, "bbb"));
        assertFalse(rocksDBSet.contains(ns, "ccc"));
        assertFalse(rocksDBSet.contains(ns, "ddd"));

        assertTrue(rocksDBSet.contains(ns2, "aaa"));
        assertTrue(rocksDBSet.contains(ns2, "bbb"));
        assertTrue(rocksDBSet.contains(ns2, "ccc"));
        assertTrue(rocksDBSet.contains(ns2, "ddd"));
        assertFalse(rocksDBSet.contains(ns2, "one"));
        assertFalse(rocksDBSet.contains(ns2, "two"));
        assertFalse(rocksDBSet.contains(ns2, "nine"));
    }

    @Test
    void addAndContains_batch() throws Exception {
        RocksMultiSet rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);
        WriteBatch writeBatch = new WriteBatch();

        rocksDBSet.addBatch(ns, writeBatch, "one");
        rocksDBSet.addBatch(ns, writeBatch, "two");
        rocksDBSet.addBatch(ns, writeBatch, "one");
        rocksDBSet.addBatch(ns, writeBatch, "nine");

        rocksDBSet.addBatch(ns2, writeBatch, "aaa");
        rocksDBSet.addBatch(ns2, writeBatch, "bbb");
        rocksDBSet.addBatch(ns2, writeBatch, "ccc");
        rocksDBSet.addBatch(ns2, writeBatch,"ddd");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertTrue(rocksDBSet.contains(ns, "one"));
        assertTrue(rocksDBSet.contains(ns, "two"));
        assertFalse(rocksDBSet.contains(ns, "three"));
        assertFalse(rocksDBSet.contains(ns, "four"));
        assertTrue(rocksDBSet.contains(ns, "nine"));
        assertFalse(rocksDBSet.contains(ns, "aaa"));
        assertFalse(rocksDBSet.contains(ns, "bbb"));
        assertFalse(rocksDBSet.contains(ns, "ccc"));
        assertFalse(rocksDBSet.contains(ns, "ddd"));

        assertTrue(rocksDBSet.contains(ns2, "aaa"));
        assertTrue(rocksDBSet.contains(ns2, "bbb"));
        assertTrue(rocksDBSet.contains(ns2, "ccc"));
        assertTrue(rocksDBSet.contains(ns2, "ddd"));
        assertFalse(rocksDBSet.contains(ns2, "one"));
        assertFalse(rocksDBSet.contains(ns2, "two"));
        assertFalse(rocksDBSet.contains(ns2, "nine"));
    }

    @Test
    void remove() throws Exception {
        RocksMultiSet rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);
        WriteBatch writeBatch = new WriteBatch();

        byte[] setName = "set1".getBytes(StandardCharsets.UTF_8);
        rocksDBSet.addBatch(setName, writeBatch, "one");
        rocksDBSet.addBatch(setName, writeBatch, "two");
        rocksDBSet.addBatch(setName, writeBatch, "one");
        rocksDBSet.addBatch(setName, writeBatch, "nine");

        rocksDBSet.removeBatch(setName, writeBatch,"one");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        Set<String> members = rocksDBSet.members(setName);
        assertThat(members).hasSize(2);
        assertThat(members).contains("two", "nine");
    }

    @Test
    void remove_batch() throws Exception {
        RocksMultiSet rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);
        byte[] setName = "set2".getBytes(StandardCharsets.UTF_8);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBSet.addBatch(setName, writeBatch, "one");
        rocksDBSet.addBatch(setName, writeBatch, "two");
        rocksDBSet.addBatch(setName, writeBatch, "one");
        rocksDBSet.addBatch(setName, writeBatch, "nine");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        rocksDBSet.remove(setName, "one");

        Set<String> members = rocksDBSet.members(setName);
        assertThat(members).hasSize(2);
        assertThat(members).contains("two", "nine");
    }

    @Test
    void members() {
        RocksMultiSet rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);
        byte[] setName = "set3".getBytes(StandardCharsets.UTF_8);
        rocksDBSet.add(setName, "one");
        rocksDBSet.add(setName,"two");
        rocksDBSet.add(setName,"one");
        rocksDBSet.add(setName,"nine");

        Set<String> members = rocksDBSet.members(setName);
        assertEquals(3, members.size());
        assertThat(members).contains("one", "two", "nine");
    }

    @Test
    void membersIterable() throws Exception {
        RocksMultiSet<String> rocksDBSet = new RocksMultiSet(rocksDBConfig, "list-cf", String.class);

        byte[] setName = "set1".getBytes(StandardCharsets.UTF_8);
        rocksDBSet.add(setName, "one");
        rocksDBSet.add(setName,"two");
        rocksDBSet.add(setName,"one");
        rocksDBSet.add(setName,"nine");
        rocksDBSet.add(setName,"ten");
        rocksDBSet.add(setName,"eleven");
        rocksDBSet.add(setName,"twelve");

        byte[] setName2 = "set2".getBytes(StandardCharsets.UTF_8);
        rocksDBSet.add(setName2, "13");
        rocksDBSet.add(setName2,"14");
        rocksDBSet.add(setName2,"15");

        var iterator = rocksDBSet.membersIterator(setName);
        var members1 = new ArrayList<>();
        while(iterator.hasNext()) {
            var item = iterator.next();
            System.out.println(item);
            members1.add(item);
        }

        iterator.close();

        iterator = rocksDBSet.membersIterator(setName2);
        var members2 = new ArrayList<>();
        while(iterator.hasNext()) {
            var item = iterator.next();
            System.out.println(item);
            members2.add(item);
        }

        iterator.close();

        assertThat(members1).hasSize(6);
        assertThat(members1).contains("one", "two", "nine", "ten", "eleven", "twelve");
        assertThat(members2).hasSize(3);
        assertThat(members2).contains("13", "14", "15");

    }
}
