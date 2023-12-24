package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RocksMultiListTest extends RocksBaseTest {
    byte[] ns = "ns1".getBytes();

    @Override
    public String getColumnFamilies() {
        return "list-cf";
    }

    @Test
    void add() {
        var list = new RocksMultiList<>(rocksDBConfig, "list-cf", "list1", String.class);
        list.add(ns, "one");
        list.add(ns, "two");
        list.add(ns, "three");

        assertEquals(3, list.size(ns));
    }

    @Test
    void get() {
        var list = new RocksMultiList<String>(rocksDBConfig, "list-cf", "list1", String.class);
        list.add(ns, "one");
        list.add(ns, "two");
        list.add(ns, "three");
        list.add(ns, "four");

        assertEquals(4, list.size(ns));
        assertThat(list.get(ns, 0)).isEqualTo("one");
        assertThat(list.get(ns, 1)).isEqualTo("two");
        assertThat(list.get(ns, 2)).isEqualTo("three");
        assertThat(list.get(ns, 3)).isEqualTo("four");
    }

    @Test
    void get_objects() {
        var list = new RocksMultiList<RocksListTest.A>(rocksDBConfig, "list-cf", "list1", RocksListTest.A.class);
        list.add(ns, new RocksListTest.A(1));
        list.add(ns, new RocksListTest.A(2));
        list.add(ns, new RocksListTest.A(3));
        list.add(ns, new RocksListTest.A(4));

        assertEquals(4, list.size(ns));
        assertThat(list.get(ns,0).value()).isEqualTo(1);
        assertThat(list.get(ns,1).value()).isEqualTo(2);
        assertThat(list.get(ns,2).value()).isEqualTo(3);
        assertThat(list.get(ns,3).value()).isEqualTo(4);
    }


    @Test
    void get_objects_defaultCF() {
        var list = new RocksMultiList<RocksListTest.A>(rocksDBConfig, "list1", RocksListTest.A.class);
        list.add(ns,new RocksListTest.A(1));
        list.add(ns,new RocksListTest.A(2));
        list.add(ns,new RocksListTest.A(3));
        list.add(ns,new RocksListTest.A(4));

        var list2 = new RocksMultiList<String>(rocksDBConfig, "list2", String.class);
        list2.add(ns,"one");
        list2.add(ns,"two");
        list2.add(ns,"three");

        assertEquals(4, list.size(ns));
        assertThat(list.get(ns,0).value()).isEqualTo(1);
        assertThat(list.get(ns,1).value()).isEqualTo(2);
        assertThat(list.get(ns,2).value()).isEqualTo(3);
        assertThat(list.get(ns,3).value()).isEqualTo(4);

        assertEquals(3, list2.size(ns));
        assertThat(list2.get(ns,0)).isEqualTo("one");
        assertThat(list2.get(ns,1)).isEqualTo("two");
        assertThat(list2.get(ns,2)).isEqualTo("three");
    }

    @Test
    void get_objects_writeBatch() throws Exception {
        var list = new RocksMultiList<RocksListTest.A>(rocksDBConfig, "list-cf", "list1", RocksListTest.A.class);
        WriteBatch writeBatch = new WriteBatch();
        list.addBatch(ns, writeBatch, new RocksListTest.A(1), new RocksListTest.A(2), new RocksListTest.A(3), new RocksListTest.A(4));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertEquals(4, list.size(ns));
        assertThat(list.get(ns,0).value()).isEqualTo(1);
        assertThat(list.get(ns,1).value()).isEqualTo(2);
        assertThat(list.get(ns,2).value()).isEqualTo(3);
        assertThat(list.get(ns,3).value()).isEqualTo(4);
    }

}
