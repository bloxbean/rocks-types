package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RocksListTest extends RocksBaseTest {

    @Override
    public String getColumnFamilies() {
        return "list-cf";
    }

    @Test
    void add() {
        var list = new RocksList(rocksDBConfig, "list-cf", "list1", String.class);
        list.add("one");
        list.add("two");
        list.add("three");

        assertEquals(3, list.size());
    }

    @Test
    void get() {
        var list = new RocksList(rocksDBConfig, "list-cf", "list1", String.class);
        list.add("one");
        list.add("two");
        list.add("three");
        list.add("four");

        assertEquals(4, list.size());
        assertThat(list.get(0)).isEqualTo("one");
        assertThat(list.get(1)).isEqualTo("two");
        assertThat(list.get(2)).isEqualTo("three");
        assertThat(list.get(3)).isEqualTo("four");
    }

    @Test
    void get_objects() {
        var list = new RocksList<A>(rocksDBConfig, "list-cf", "list1", A.class);
        list.add(new A(1));
        list.add(new A(2));
        list.add(new A(3));
        list.add(new A(4));

        assertEquals(4, list.size());
        assertThat(list.get(0).value).isEqualTo(1);
        assertThat(list.get(1).value).isEqualTo(2);
        assertThat(list.get(2).value).isEqualTo(3);
        assertThat(list.get(3).value).isEqualTo(4);
    }


    @Test
    void get_objects_defaultCF() {
        var list = new RocksList<A>(rocksDBConfig, "list1", A.class);
        list.add(new A(1));
        list.add(new A(2));
        list.add(new A(3));
        list.add(new A(4));

        var list2 = new RocksList<String>(rocksDBConfig, "list2", String.class);
        list2.add("one");
        list2.add("two");
        list2.add("three");

        assertEquals(4, list.size());
        assertThat(list.get(0).value).isEqualTo(1);
        assertThat(list.get(1).value).isEqualTo(2);
        assertThat(list.get(2).value).isEqualTo(3);
        assertThat(list.get(3).value).isEqualTo(4);

        assertEquals(3, list2.size());
        assertThat(list2.get(0)).isEqualTo("one");
        assertThat(list2.get(1)).isEqualTo("two");
        assertThat(list2.get(2)).isEqualTo("three");
    }

    @Test
    void get_objects_writeBatch() throws Exception {
        var list = new RocksList<A>(rocksDBConfig, "list-cf", "list1", A.class);
        WriteBatch writeBatch = new WriteBatch();
        list.addBatch(writeBatch, new A(1), new A(2), new A(3), new A(4));

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertEquals(4, list.size());
        assertThat(list.get(0).value).isEqualTo(1);
        assertThat(list.get(1).value).isEqualTo(2);
        assertThat(list.get(2).value).isEqualTo(3);
        assertThat(list.get(3).value).isEqualTo(4);
    }


    record A(int value) {}
}
