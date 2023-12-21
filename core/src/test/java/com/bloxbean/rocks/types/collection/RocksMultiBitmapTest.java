package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksMultiBitmapTest extends RocksBaseTest {

    @Test
    void setBit() {
        for (int i=0; i < 100; i++) {
            var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * ( i + 1));
            rocksBitMap.setBit("ns1", 1);
            rocksBitMap.setBit("ns1", 50);
            rocksBitMap.setBit("ns1", 99);

            rocksBitMap.setBit("ns1", 101);
            rocksBitMap.setBit("ns1", 150);
            rocksBitMap.setBit("ns1", 199);

            rocksBitMap.setBit("ns1", 201);
            rocksBitMap.setBit("ns1", 250);
            rocksBitMap.setBit("ns1", 999999899);

            assertThat(rocksBitMap.getBit("ns1", 1)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 50)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 99)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 101)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 150)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 199)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 201)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 250)).isTrue();

            assertThat(rocksBitMap.getBit("ns1", 41)).isFalse();
            assertThat(rocksBitMap.getBit("ns1", 100)).isFalse();
            assertThat(rocksBitMap.getBit("ns1", 200)).isFalse();
            assertThat(rocksBitMap.getBit("ns1", 240)).isFalse();
            assertThat(rocksBitMap.getBit("ns1", 350)).isFalse();
            assertThat(rocksBitMap.getBit("ns1", 999999899)).isTrue();
            assertThat(rocksBitMap.getBit("ns1", 999999898)).isFalse();
        }
    }

    @Test
    void setBit_batch() throws Exception {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 20);
        WriteBatch writeBatch = new WriteBatch();
        rocksBitMap.setBitBatch("ns1", writeBatch, 1, 50, 99, 101, 150, 199, 201, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit("ns1", 1)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 50)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 99)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 101)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 150)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 199)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 201)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 250)).isTrue();

        assertThat(rocksBitMap.getBit("ns1", 41)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 100)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 200)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 240)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 350)).isFalse();
    }

    @Test
    void clearBit() {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap");
        rocksBitMap.setBit("ns1", 1);
        rocksBitMap.setBit("ns1", 50);
        rocksBitMap.setBit("ns1", 99);

        rocksBitMap.setBit("ns1", 101);
        rocksBitMap.setBit("ns1", 150);
        rocksBitMap.setBit("ns1", 199);

        rocksBitMap.setBit("ns1", 201);
        rocksBitMap.setBit("ns1", 250);

        rocksBitMap.clearBit("ns1", 99);
        rocksBitMap.clearBit("ns1", 250);

        assertThat(rocksBitMap.getBit("ns1", 1)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 50)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 99)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 101)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 150)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 199)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 201)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 250)).isFalse();

    }

    @Test
    void clearBit_batch() throws Exception {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit("ns1", 1);
        rocksBitMap.setBit("ns1", 50);
        rocksBitMap.setBit("ns1", 99);

        rocksBitMap.setBit("ns1", 101);
        rocksBitMap.setBit("ns1", 150);
        rocksBitMap.setBit("ns1", 199);

        rocksBitMap.setBit("ns1", 201);
        rocksBitMap.setBit("ns1", 250);

        var writeBatch = new WriteBatch();
        rocksBitMap.clearBitBatch("ns1", writeBatch, 99, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit("ns1", 1)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 50)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 99)).isFalse();
        assertThat(rocksBitMap.getBit("ns1", 101)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 150)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 199)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 201)).isTrue();
        assertThat(rocksBitMap.getBit("ns1", 250)).isFalse();

    }

    @Test
    void getAllBits() {
        var rocksBitMap = new RocksMultiBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit("ns1", 1);
        rocksBitMap.setBit("ns1", 50);
        rocksBitMap.setBit("ns1", 99);

        rocksBitMap.setBit("ns1", 101);
        rocksBitMap.setBit("ns1", 150);
        rocksBitMap.setBit("ns1", 199);

        rocksBitMap.setBit("ns1", 201);
        rocksBitMap.setBit("ns1", 250);

        var allBits = rocksBitMap.getAllBits("ns1");

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
        rocksBitMap.setBit("ns1", 1);
        rocksBitMap.setBit("ns1", 50);
        rocksBitMap.setBit("ns1", 99);

        rocksBitMap.setBit("ns1", 101);
        rocksBitMap.setBit("ns1", 150);
        rocksBitMap.setBit("ns1", 199);

        rocksBitMap.setBit("ns1", 201);
        rocksBitMap.setBit("ns1", 250);

        var bits = rocksBitMap.getBits("ns1", 2, 5);

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
            rocksBitMap.setBit("ns1", 1);
            rocksBitMap.setBit("ns1", 50);
            rocksBitMap.setBit("ns1", 99);

            rocksBitMap.setBit("ns1", 101);
            rocksBitMap.setBit("ns1", 150);
            rocksBitMap.setBit("ns1", 199);

            rocksBitMap.setBit("ns1", 201);
            rocksBitMap.setBit("ns1", 250);

            long index1 = rocksBitMap.nextSetBit("ns1", 140);
            long index2 = rocksBitMap.nextSetBit("ns1", 199);
            long index3 = rocksBitMap.nextSetBit("ns1", 202);
            long index4 = rocksBitMap.nextSetBit("ns1", 250);
            long index5 = rocksBitMap.nextSetBit("ns1", 251);

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
            rocksBitMap.setBit("ns1", 1);
            rocksBitMap.setBit("ns1", 50);
            rocksBitMap.setBit("ns1", 99);

            rocksBitMap.setBit("ns1", 101);
            rocksBitMap.setBit("ns1", 150);
            rocksBitMap.setBit("ns1", 199);

            rocksBitMap.setBit("ns1", 201);
            rocksBitMap.setBit("ns1", 250);

            long index1 = rocksBitMap.nextClearBit("ns1", 140);
            long index2 = rocksBitMap.nextClearBit("ns1", 199);
            long index3 = rocksBitMap.nextClearBit("ns1", 202);
            long index4 = rocksBitMap.nextClearBit("ns1", 250);
            long index5 = rocksBitMap.nextClearBit("ns1", 251);

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
            rocksBitMap.setBit("ns1", 1);
            rocksBitMap.setBit("ns1", 50);
            rocksBitMap.setBit("ns1", 99);

            rocksBitMap.setBit("ns1", 101);
            rocksBitMap.setBit("ns1", 150);
            rocksBitMap.setBit("ns1", 199);

            rocksBitMap.setBit("ns1", 201);
            rocksBitMap.setBit("ns1", 250);

            long index1 = rocksBitMap.previousSetBit("ns1", 140);
            long index2 = rocksBitMap.previousSetBit("ns1", 199);
            long index3 = rocksBitMap.previousSetBit("ns1", 202);
            long index4 = rocksBitMap.previousSetBit("ns1", 250);
            long index5 = rocksBitMap.previousSetBit("ns1", 251);
            long index6 = rocksBitMap.previousSetBit("ns1", 49);
            long index7 = rocksBitMap.previousSetBit("ns1", 1);
            long index8 = rocksBitMap.previousSetBit("ns1", 0);

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
            rocksBitMap.setBit("ns1", 1);
            rocksBitMap.setBit("ns1", 50);
            rocksBitMap.setBit("ns1", 99);

            rocksBitMap.setBit("ns1", 101);
            rocksBitMap.setBit("ns1", 150);
            rocksBitMap.setBit("ns1", 199);

            rocksBitMap.setBit("ns1", 201);
            rocksBitMap.setBit("ns1", 250);

            System.out.println(i);

            long index1 = rocksBitMap.previousClearBit("ns1", 140);
            long index2 = rocksBitMap.previousClearBit("ns1", 199);
            long index3 = rocksBitMap.previousClearBit("ns1", 202);
            long index4 = rocksBitMap.previousClearBit("ns1", 250);
            long index5 = rocksBitMap.previousClearBit("ns1", 251);
            long index6 = rocksBitMap.previousClearBit("ns1", 49);
            long index7 = rocksBitMap.previousClearBit("ns1", 1);
            long index8 = rocksBitMap.previousClearBit("ns1", 0);

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
