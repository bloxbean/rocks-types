package com.bloxbean.rocks.types.collection;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RocksBitmapTest extends RocksBaseTest {

    @Test
    void setBit() {
        for (int i = 0; i < 100; i++) {
            var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(1);
            rocksBitMap.setBit(50);
            rocksBitMap.setBit(99);

            rocksBitMap.setBit(101);
            rocksBitMap.setBit(150);
            rocksBitMap.setBit(199);

            rocksBitMap.setBit(201);
            rocksBitMap.setBit(250);

            assertThat(rocksBitMap.getBit(1)).isTrue();
            assertThat(rocksBitMap.getBit(50)).isTrue();
            assertThat(rocksBitMap.getBit(99)).isTrue();
            assertThat(rocksBitMap.getBit(101)).isTrue();
            assertThat(rocksBitMap.getBit(150)).isTrue();
            assertThat(rocksBitMap.getBit(199)).isTrue();
            assertThat(rocksBitMap.getBit(201)).isTrue();
            assertThat(rocksBitMap.getBit(250)).isTrue();

            assertThat(rocksBitMap.getBit(41)).isFalse();
            assertThat(rocksBitMap.getBit(100)).isFalse();
            assertThat(rocksBitMap.getBit(200)).isFalse();
            assertThat(rocksBitMap.getBit(240)).isFalse();
            assertThat(rocksBitMap.getBit(350)).isFalse();
        }
    }

    @Test
    void setBit_batch() throws Exception {
        var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap", 20);
        WriteBatch writeBatch = new WriteBatch();
        rocksBitMap.setBitBatch(writeBatch, 1, 50, 99, 101, 150, 199, 201, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit(1)).isTrue();
        assertThat(rocksBitMap.getBit(50)).isTrue();
        assertThat(rocksBitMap.getBit(99)).isTrue();
        assertThat(rocksBitMap.getBit(101)).isTrue();
        assertThat(rocksBitMap.getBit(150)).isTrue();
        assertThat(rocksBitMap.getBit(199)).isTrue();
        assertThat(rocksBitMap.getBit(201)).isTrue();
        assertThat(rocksBitMap.getBit(250)).isTrue();

        assertThat(rocksBitMap.getBit(41)).isFalse();
        assertThat(rocksBitMap.getBit(100)).isFalse();
        assertThat(rocksBitMap.getBit(200)).isFalse();
        assertThat(rocksBitMap.getBit(240)).isFalse();
        assertThat(rocksBitMap.getBit(350)).isFalse();
    }

    @Test
    void clearBit() {
        var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap");
        rocksBitMap.setBit(1);
        rocksBitMap.setBit(50);
        rocksBitMap.setBit(99);

        rocksBitMap.setBit(101);
        rocksBitMap.setBit(150);
        rocksBitMap.setBit(199);

        rocksBitMap.setBit(201);
        rocksBitMap.setBit(250);

        rocksBitMap.clearBit(99);
        rocksBitMap.clearBit(250);

        assertThat(rocksBitMap.getBit(1)).isTrue();
        assertThat(rocksBitMap.getBit(50)).isTrue();
        assertThat(rocksBitMap.getBit(99)).isFalse();
        assertThat(rocksBitMap.getBit(101)).isTrue();
        assertThat(rocksBitMap.getBit(150)).isTrue();
        assertThat(rocksBitMap.getBit(199)).isTrue();
        assertThat(rocksBitMap.getBit(201)).isTrue();
        assertThat(rocksBitMap.getBit(250)).isFalse();

    }

    @Test
    void clearBit_batch() throws Exception {
        var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(1);
        rocksBitMap.setBit(50);
        rocksBitMap.setBit(99);

        rocksBitMap.setBit(101);
        rocksBitMap.setBit(150);
        rocksBitMap.setBit(199);

        rocksBitMap.setBit(201);
        rocksBitMap.setBit(250);

        var writeBatch = new WriteBatch();
        rocksBitMap.clearBitBatch(writeBatch, 99, 250);

        rocksDBConfig.getRocksDB().write(new WriteOptions(), writeBatch);

        assertThat(rocksBitMap.getBit(1)).isTrue();
        assertThat(rocksBitMap.getBit(50)).isTrue();
        assertThat(rocksBitMap.getBit(99)).isFalse();
        assertThat(rocksBitMap.getBit(101)).isTrue();
        assertThat(rocksBitMap.getBit(150)).isTrue();
        assertThat(rocksBitMap.getBit(199)).isTrue();
        assertThat(rocksBitMap.getBit(201)).isTrue();
        assertThat(rocksBitMap.getBit(250)).isFalse();

    }

    @Test
    void getAllBits() {
        var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(1);
        rocksBitMap.setBit(50);
        rocksBitMap.setBit(99);

        rocksBitMap.setBit(101);
        rocksBitMap.setBit(150);
        rocksBitMap.setBit(199);

        rocksBitMap.setBit(201);
        rocksBitMap.setBit(250);

        var allBits = rocksBitMap.getAllBits();

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
        var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap", 30);
        rocksBitMap.setBit(1);
        rocksBitMap.setBit(50);
        rocksBitMap.setBit(99);

        rocksBitMap.setBit(101);
        rocksBitMap.setBit(150);
        rocksBitMap.setBit(199);

        rocksBitMap.setBit(201);
        rocksBitMap.setBit(250);

        var bits = rocksBitMap.getBits(2, 5);

        var expectedBits = new RoaringBitmap();
        expectedBits.add(99);
        expectedBits.add(101);
        expectedBits.add(150);

        assertThat(bits).isEqualTo(expectedBits);
    }

    @Test
    void nextSetBit() {
        for (int i = 0; i < 10; i++) {
            var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap-" + i, (10 * (i + 1)));
            rocksBitMap.setBit(1);
            rocksBitMap.setBit(50);
            rocksBitMap.setBit(99);

            rocksBitMap.setBit(101);
            rocksBitMap.setBit(150);
            rocksBitMap.setBit(199);

            rocksBitMap.setBit(201);
            rocksBitMap.setBit(250);

            long index1 = rocksBitMap.nextSetBit(140);
            long index2 = rocksBitMap.nextSetBit(199);
            long index3 = rocksBitMap.nextSetBit(202);
            long index4 = rocksBitMap.nextSetBit(250);
            long index5 = rocksBitMap.nextSetBit(251);

            assertThat(index1).isEqualTo(150);
            assertThat(index2).isEqualTo(199);
            assertThat(index3).isEqualTo(250);
            assertThat(index4).isEqualTo(250);
            assertThat(index5).isEqualTo(-1);
        }
    }

    @Test
    void nextClearBit() {
        for (int i = 0; i < 10; i++) {
            System.out.println(i);
            var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(1);
            rocksBitMap.setBit(50);
            rocksBitMap.setBit(99);

            rocksBitMap.setBit(101);
            rocksBitMap.setBit(150);
            rocksBitMap.setBit(199);

            rocksBitMap.setBit(201);
            rocksBitMap.setBit(250);

            long index1 = rocksBitMap.nextClearBit(140);
            long index2 = rocksBitMap.nextClearBit(199);
            long index3 = rocksBitMap.nextClearBit(202);
            long index4 = rocksBitMap.nextClearBit(250);
            long index5 = rocksBitMap.nextClearBit(251);

            assertThat(index1).isEqualTo(140);
            assertThat(index2).isEqualTo(200);
            assertThat(index3).isEqualTo(202);
            assertThat(index4).isEqualTo(251);
            assertThat(index5).isEqualTo(251);
        }
    }

    @Test
    void prevSetBit() {
        for (int i = 0; i < 10; i++) {
            var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(1);
            rocksBitMap.setBit(50);
            rocksBitMap.setBit(99);

            rocksBitMap.setBit(101);
            rocksBitMap.setBit(150);
            rocksBitMap.setBit(199);

            rocksBitMap.setBit(201);
            rocksBitMap.setBit(250);

            long index1 = rocksBitMap.previousSetBit(140);
            long index2 = rocksBitMap.previousSetBit(199);
            long index3 = rocksBitMap.previousSetBit(202);
            long index4 = rocksBitMap.previousSetBit(250);
            long index5 = rocksBitMap.previousSetBit(251);
            long index6 = rocksBitMap.previousSetBit(49);
            long index7 = rocksBitMap.previousSetBit(1);
            long index8 = rocksBitMap.previousSetBit(0);

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
        for (int i = 0; i < 10; i++) {
            var rocksBitMap = new RocksBitmap(rocksDBConfig, "test-bitmap-" + i, 10 * (i + 1));
            rocksBitMap.setBit(1);
            rocksBitMap.setBit(50);
            rocksBitMap.setBit(99);

            rocksBitMap.setBit(101);
            rocksBitMap.setBit(150);
            rocksBitMap.setBit(199);

            rocksBitMap.setBit(201);
            rocksBitMap.setBit(250);

            System.out.println(i);

            long index1 = rocksBitMap.previousClearBit(140);
            long index2 = rocksBitMap.previousClearBit(199);
            long index3 = rocksBitMap.previousClearBit(202);
            long index4 = rocksBitMap.previousClearBit(250);
            long index5 = rocksBitMap.previousClearBit(251);
            long index6 = rocksBitMap.previousClearBit(49);
            long index7 = rocksBitMap.previousClearBit(1);
            long index8 = rocksBitMap.previousClearBit(0);

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
