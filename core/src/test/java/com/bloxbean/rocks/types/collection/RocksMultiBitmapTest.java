package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksMultiBitmapTest extends RocksBaseTest {

    private byte[] ns = "ns1".getBytes();

    @Test
    void setBit() {
        for (int i=0; i < 100; i++) {
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * ( i + 1));
            rocksBitMap.setBit(ns, 1);
            rocksBitMap.setBit(ns, 50);
            rocksBitMap.setBit(ns, 99);

            rocksBitMap.setBit(ns, 101);
            rocksBitMap.setBit(ns, 150);
            rocksBitMap.setBit(ns, 199);

            rocksBitMap.setBit(ns, 201);
            rocksBitMap.setBit(ns, 250);
            rocksBitMap.setBit(ns, 999999899);

            assertThat(rocksBitMap.getBit(ns, 1)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 50)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 99)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 101)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 150)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 199)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 201)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 250)).isTrue();

            assertThat(rocksBitMap.getBit(ns, 41)).isFalse();
            assertThat(rocksBitMap.getBit(ns, 100)).isFalse();
            assertThat(rocksBitMap.getBit(ns, 200)).isFalse();
            assertThat(rocksBitMap.getBit(ns, 240)).isFalse();
            assertThat(rocksBitMap.getBit(ns, 350)).isFalse();
            assertThat(rocksBitMap.getBit(ns, 999999899)).isTrue();
            assertThat(rocksBitMap.getBit(ns, 999999898)).isFalse();
        }
    }

    @Test
    void setBit_batch() throws Exception {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 20);
        WriteBatch writeBatch = new WriteBatch();
        rocksBitMap.setBitBatch(ns, writeBatch, 1, 50, 99, 101, 150, 199, 201, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit(ns, 1)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 50)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 99)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 101)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 150)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 199)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 201)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 250)).isTrue();

        assertThat(rocksBitMap.getBit(ns, 41)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 100)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 200)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 240)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 350)).isFalse();
    }

    @Test
    void clearBit() {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap");
        rocksBitMap.setBit(ns, 1);
        rocksBitMap.setBit(ns, 50);
        rocksBitMap.setBit(ns, 99);

        rocksBitMap.setBit(ns, 101);
        rocksBitMap.setBit(ns, 150);
        rocksBitMap.setBit(ns, 199);

        rocksBitMap.setBit(ns, 201);
        rocksBitMap.setBit(ns, 250);

        rocksBitMap.clearBit(ns, 99);
        rocksBitMap.clearBit(ns, 250);

        assertThat(rocksBitMap.getBit(ns, 1)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 50)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 99)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 101)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 150)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 199)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 201)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 250)).isFalse();

    }

    @Test
    void clearBit_batch() throws Exception {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(ns, 1);
        rocksBitMap.setBit(ns, 50);
        rocksBitMap.setBit(ns, 99);

        rocksBitMap.setBit(ns, 101);
        rocksBitMap.setBit(ns, 150);
        rocksBitMap.setBit(ns, 199);

        rocksBitMap.setBit(ns, 201);
        rocksBitMap.setBit(ns, 250);

        var writeBatch = new WriteBatch();
        rocksBitMap.clearBitBatch(ns, writeBatch, 99, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit(ns, 1)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 50)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 99)).isFalse();
        assertThat(rocksBitMap.getBit(ns, 101)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 150)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 199)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 201)).isTrue();
        assertThat(rocksBitMap.getBit(ns, 250)).isFalse();

    }

    @Test
    void getAllBits() {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(ns, 1);
        rocksBitMap.setBit(ns, 50);
        rocksBitMap.setBit(ns, 99);

        rocksBitMap.setBit(ns, 101);
        rocksBitMap.setBit(ns, 150);
        rocksBitMap.setBit(ns, 199);

        rocksBitMap.setBit(ns, 201);
        rocksBitMap.setBit(ns, 250);

        var allBits = rocksBitMap.getAllBits(ns);

        var expectedBitSet = new RoaringBitmap();
        expectedBitSet.add(1);
        expectedBitSet.add(50);
        expectedBitSet.add(99);
        expectedBitSet.add(101);
        expectedBitSet.add(150);
        expectedBitSet.add(199);
        expectedBitSet.add(201);
        expectedBitSet.add(250);

        System.out.println(allBits);

        assertThat(allBits).isEqualTo(expectedBitSet);
    }

    @Test
    void getBits() {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(ns, 1);
        rocksBitMap.setBit(ns, 50);
        rocksBitMap.setBit(ns, 99);

        rocksBitMap.setBit(ns, 101);
        rocksBitMap.setBit(ns, 150);
        rocksBitMap.setBit(ns, 199);

        rocksBitMap.setBit(ns, 201);
        rocksBitMap.setBit(ns, 250);

        var bits = rocksBitMap.getBits(ns, 2, 5);

        var expectedBits = new RoaringBitmap();
        expectedBits.add(99);
        expectedBits.add(101);
        expectedBits.add(150);

        assertThat(bits).isEqualTo(expectedBits);
    }

    @Test
    void nextSetBit() {
        for (int i=0; i < 10; i++) {
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, (10 * (i + 1)));
            rocksBitMap.setBit(ns, 1);
            rocksBitMap.setBit(ns, 50);
            rocksBitMap.setBit(ns, 99);

            rocksBitMap.setBit(ns, 101);
            rocksBitMap.setBit(ns, 150);
            rocksBitMap.setBit(ns, 199);

            rocksBitMap.setBit(ns, 201);
            rocksBitMap.setBit(ns, 250);

            long index1 = rocksBitMap.nextSetBit(ns, 140);
            long index2 = rocksBitMap.nextSetBit(ns, 199);
            long index3 = rocksBitMap.nextSetBit(ns, 202);
            long index4 = rocksBitMap.nextSetBit(ns, 250);
            long index5 = rocksBitMap.nextSetBit(ns, 251);

            assertThat(index1).isEqualTo(150);
            assertThat(index2).isEqualTo(199);
            assertThat(index3).isEqualTo(250);
            assertThat(index4).isEqualTo(250);
            assertThat(index5).isEqualTo(-1);
        }
    }

    @Test
    void nextClearBit() {
        for (int i=0; i < 10; i++) {
            System.out.println(i);
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(ns, 1);
            rocksBitMap.setBit(ns, 50);
            rocksBitMap.setBit(ns, 99);

            rocksBitMap.setBit(ns, 101);
            rocksBitMap.setBit(ns, 150);
            rocksBitMap.setBit(ns, 199);

            rocksBitMap.setBit(ns, 201);
            rocksBitMap.setBit(ns, 250);

            long index1 = rocksBitMap.nextClearBit(ns, 140);
            long index2 = rocksBitMap.nextClearBit(ns, 199);
            long index3 = rocksBitMap.nextClearBit(ns, 202);
            long index4 = rocksBitMap.nextClearBit(ns, 250);
            long index5 = rocksBitMap.nextClearBit(ns, 251);

            assertThat(index1).isEqualTo(140);
            assertThat(index2).isEqualTo(200);
            assertThat(index3).isEqualTo(202);
            assertThat(index4).isEqualTo(251);
            assertThat(index5).isEqualTo(251);
        }
    }

    @Test
    void prevSetBit() {
        for (int i=0; i < 10; i++) {
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(ns, 1);
            rocksBitMap.setBit(ns, 50);
            rocksBitMap.setBit(ns, 99);

            rocksBitMap.setBit(ns, 101);
            rocksBitMap.setBit(ns, 150);
            rocksBitMap.setBit(ns, 199);

            rocksBitMap.setBit(ns, 201);
            rocksBitMap.setBit(ns, 250);

            long index1 = rocksBitMap.previousSetBit(ns, 140);
            long index2 = rocksBitMap.previousSetBit(ns, 199);
            long index3 = rocksBitMap.previousSetBit(ns, 202);
            long index4 = rocksBitMap.previousSetBit(ns, 250);
            long index5 = rocksBitMap.previousSetBit(ns, 251);
            long index6 = rocksBitMap.previousSetBit(ns, 49);
            long index7 = rocksBitMap.previousSetBit(ns, 1);
            long index8 = rocksBitMap.previousSetBit(ns, 0);

            assertThat(index1).isEqualTo(101);
            assertThat(index2).isEqualTo(199);
            assertThat(index3).isEqualTo(201);
            assertThat(index4).isEqualTo(250);
            assertThat(index5).isEqualTo(250);
            assertThat(index6).isEqualTo(1);
            assertThat(index7).isEqualTo(1);
            assertThat(index8).isEqualTo(-1);
        }
    }

    @Test
    void prevClearBit() {
        for (int i=0; i < 10; i++) {
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(ns, 1);
            rocksBitMap.setBit(ns, 50);
            rocksBitMap.setBit(ns, 99);

            rocksBitMap.setBit(ns, 101);
            rocksBitMap.setBit(ns, 150);
            rocksBitMap.setBit(ns, 199);

            rocksBitMap.setBit(ns, 201);
            rocksBitMap.setBit(ns, 250);

            System.out.println(i);

            long index1 = rocksBitMap.previousClearBit(ns, 140);
            long index2 = rocksBitMap.previousClearBit(ns, 199);
            long index3 = rocksBitMap.previousClearBit(ns, 202);
            long index4 = rocksBitMap.previousClearBit(ns, 250);
            long index5 = rocksBitMap.previousClearBit(ns, 251);
            long index6 = rocksBitMap.previousClearBit(ns, 49);
            long index7 = rocksBitMap.previousClearBit(ns, 1);
            long index8 = rocksBitMap.previousClearBit(ns, 0);

            assertThat(index1).isEqualTo(140);
            assertThat(index2).isEqualTo(198);
            assertThat(index3).isEqualTo(202);
            assertThat(index4).isEqualTo(249);
            assertThat(index5).isEqualTo(251);
            assertThat(index6).isEqualTo(49);
            assertThat(index7).isEqualTo(0);
            assertThat(index8).isEqualTo(0);
        }
    }
}
