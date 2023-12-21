package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.config.RocksDBConfig;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.WriteBatch;

/**
 * Bitmap implementation using RocksDB
 * RocksBitmap is a wrapper class for RocksMultiBitmap with default column family
 */
public class RocksBitmap extends RocksMultiBitmap {

    public RocksBitmap(RocksDBConfig rocksDBConfig, String name) {
        super(rocksDBConfig, name);
    }

    public RocksBitmap(RocksDBConfig rocksDBConfig, String name, int fragmentSize) {
        super(rocksDBConfig, name, fragmentSize);
    }

    public RocksBitmap(RocksDBConfig rocksDBConfig, String columnFamily, String name) {
        super(rocksDBConfig, columnFamily, name);
    }

    public RocksBitmap(RocksDBConfig rocksDBConfig, String columnFamily, String name, int fragmentSize) {
        super(rocksDBConfig, columnFamily, name, fragmentSize);
    }

    public void setBit(int index) {
        super.setBit(null, index);
    }

    public void setBitBatch(WriteBatch writeBatch, int... bitIndexes) {
        super.setBitBatch(null, writeBatch, bitIndexes);
    }

    public boolean getBit(int bitIndex) {
        return super.getBit(null, bitIndex);
    }

    public void clearBit(int bitIndex) {
        super.clearBit(null, bitIndex);
    }

    public void clearBitBatch(WriteBatch writeBatch, int... bitIndexes) {
        super.clearBitBatch(null, writeBatch, bitIndexes);
    }

    public long nextSetBit(int fromIndex) {
        return super.nextSetBit(null, fromIndex);
    }

    public long nextClearBit(int fromIndex) {
        return super.nextClearBit(null, fromIndex);
    }

    public long previousSetBit(int fromIndex) {
        return super.previousSetBit(null, fromIndex);
    }

    public long previousClearBit(int fromIndex) {
        return super.previousClearBit(null, fromIndex);
    }

    public RoaringBitmap getAllBits() {
        return super.getAllBits(null);
    }

    public RoaringBitmap getBits(int fromFragmentIndex, int toFragmentIndex) {
        return super.getBits(null, fromFragmentIndex, toFragmentIndex);
    }
}
