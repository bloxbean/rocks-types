package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RocksSetTest extends RocksBaseTest {

    @Override
    public String getColumnFamilies() {
        return "list1";
    }

    @Test
    void addAndContains() {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        rocksDBSet.add("one");
        rocksDBSet.add("two");
        rocksDBSet.add("one");
        rocksDBSet.add("nine");

        assertTrue(rocksDBSet.contains("one"));
        assertTrue(rocksDBSet.contains("two"));
        assertFalse(rocksDBSet.contains("three"));
        assertFalse(rocksDBSet.contains("four"));
        assertTrue(rocksDBSet.contains("nine"));
    }

    @Test
    void addAndContains_batch() throws Exception {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBSet.addBatch(writeBatch, "one");
        rocksDBSet.addBatch(writeBatch, "two");
        rocksDBSet.addBatch(writeBatch, "one");
        rocksDBSet.addBatch(writeBatch, "nine");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertTrue(rocksDBSet.contains("one"));
        assertTrue(rocksDBSet.contains("two"));
        assertFalse(rocksDBSet.contains("three"));
        assertFalse(rocksDBSet.contains("four"));
        assertTrue(rocksDBSet.contains("nine"));
    }

    @Test
    void addAndContains_batch_merged() throws Exception {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBSet.addBatch(writeBatch, "one", "two", "one", "nine");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertTrue(rocksDBSet.contains("one"));
        assertTrue(rocksDBSet.contains("two"));
        assertFalse(rocksDBSet.contains("three"));
        assertFalse(rocksDBSet.contains("four"));
        assertTrue(rocksDBSet.contains("nine"));
    }

    @Test
    void remove() {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        rocksDBSet.add("one");
        rocksDBSet.add("two");
        rocksDBSet.add("one");
        rocksDBSet.add("nine");

        rocksDBSet.remove("one");

        Set<String> members = rocksDBSet.members();
        assertThat(members).hasSize(2);
        assertThat(members).contains("two", "nine");
    }

    @Test
    void remove_batch() throws Exception {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBSet.addBatch(writeBatch, "one");
        rocksDBSet.addBatch(writeBatch, "two");
        rocksDBSet.addBatch(writeBatch, "one");
        rocksDBSet.addBatch(writeBatch, "nine");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        rocksDBSet.remove("one");

        Set<String> members = rocksDBSet.members();
        assertThat(members).hasSize(2);
        assertThat(members).contains("two", "nine");
    }

    @Test
    void remove_batch_merged() throws Exception {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "set1", String.class);
        WriteBatch writeBatch = new WriteBatch();
        rocksDBSet.addBatch(writeBatch, "one", "two", "one", "nine", "five");

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksDBSet.members()).hasSize(4);

        rocksDBSet.removeBatch(writeBatch, "one", "five");
        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        Set<String> members = rocksDBSet.members();
        assertThat(members).hasSize(2);
        assertThat(members).contains("two", "nine");
    }

    @Test
    void members() {
        RocksSet rocksDBSet = new RocksSet(rocksDBConfig, "list1", "set1", String.class);
        rocksDBSet.add("one");
        rocksDBSet.add("two");
        rocksDBSet.add("one");
        rocksDBSet.add("nine");

        Set<String> members = rocksDBSet.members();
        assertEquals(3, members.size());
        assertThat(members).contains("one", "two", "nine");
    }
}
