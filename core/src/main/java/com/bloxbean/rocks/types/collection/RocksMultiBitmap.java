package com.bloxbean.rocks.types.collection;

import com.bloxbean.rocks.types.collection.metadata.BitmapMetadata;
import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.config.RocksDBConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.WriteBatch;

import java.io.*;
import java.util.Optional;

/**
 * Bitmap implementation using RocksDB. It supports multiple bitmaps under the same name but different namespaces.
 * This implementation uses RoaringBitmap as the underlying bitmap implementation.
 * <p>
 * This implementation is not thread safe.
 */
@Slf4j
public class RocksMultiBitmap extends BaseDataType {
    private static final int DEFAULT_FRAGMENT_SIZE = 1000; //1000 bits
    private int fragmentSize;

    public RocksMultiBitmap(RocksDBConfig rocksDBConfig, String name) {
        super(rocksDBConfig, name, null);
        this.fragmentSize = DEFAULT_FRAGMENT_SIZE;
    }

    public RocksMultiBitmap(RocksDBConfig rocksDBConfig, String name, int fragmentSize) {
        super(rocksDBConfig, name, null);
        this.fragmentSize = fragmentSize;
    }

    public RocksMultiBitmap(RocksDBConfig rocksDBConfig, String columnFamily, String name) {
        super(rocksDBConfig, columnFamily, name, null);
    }

    public RocksMultiBitmap(RocksDBConfig rocksDBConfig, String columnFamily, String name, int fragmentSize) {
        super(rocksDBConfig, columnFamily, name, null);
        this.fragmentSize = fragmentSize;
    }

    public void setBit(byte[] ns, int bitIndex) {
        var metadata = createMetadata(ns).orElseThrow();
        setBit(ns, null, metadata, bitIndex);
    }

    public void setBitBatch(byte[] ns, WriteBatch writeBatch, int... bitIndexes) {
        var metadata = createMetadata(ns).orElseThrow();
        for (int bitIndex : bitIndexes) {
            setBit(ns, writeBatch, metadata, bitIndex);
        }
    }

    @SneakyThrows
    private void setBit(byte[] ns, WriteBatch writeBatch, BitmapMetadata metadata, int bitIndex) {

        int fragmentIndex = bitIndex / fragmentSize;
        int fragmentBitIndex = bitIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);


        RoaringBitmap bitSet = null;
        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            bitSet = new RoaringBitmap();
        } else {
            bitSet = getRoaringBitmap(valueBytes);
        }

        bitSet.add(fragmentBitIndex);

        byte[] bytes = serializeRoaringBitmap(bitSet);

        write(writeBatch, keyBytes, bytes);
        if (fragmentIndex > metadata.getMaxFragmentIndex()) {
            metadata.setMaxFragmentIndex(fragmentIndex);
            updateMetadata(writeBatch, metadata, ns);
            log.info("Max fragment index updated to {}", metadata);
        }
    }

    private static byte[] serializeRoaringBitmap(RoaringBitmap bitSet) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bitSet.serialize(new DataOutputStream(baos));
        return baos.toByteArray();
    }

    private static RoaringBitmap getRoaringBitmap(byte[] valueBytes) throws IOException {
        RoaringBitmap bitSet;
        bitSet = new RoaringBitmap();//BitSet.valueOf(valueBytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
        bitSet.deserialize(new DataInputStream(bais));
        return bitSet;
    }

    public void clearBit(byte[] ns, int bitIndex) {
        var metadata = createMetadata(ns).orElseThrow();
        clearBit(ns, null, metadata, bitIndex);
    }

    public void clearBitBatch(byte[] ns, WriteBatch writeBatch, int... bitIndexes) {
        var metadata = createMetadata(ns).orElseThrow();
        for (int bitIndex : bitIndexes) {
            clearBit(ns, writeBatch, metadata, bitIndex);
        }
    }

    @SneakyThrows
    private void clearBit(byte[] ns, WriteBatch writeBatch, BitmapMetadata metadata, int bitIndex) {

        int fragmentIndex = bitIndex / fragmentSize;
        int fragmentBitIndex = bitIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            return; //nothing to clear
        }

        var bitSet = getRoaringBitmap(valueBytes);
        bitSet.remove(fragmentBitIndex);

        byte[] serializedBytes = serializeRoaringBitmap(bitSet);

        write(writeBatch, keyBytes, serializedBytes);
    }

    public boolean getBit(byte[] ns, int bitIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return getBit(ns, metadata, bitIndex);
    }

    @SneakyThrows
    private boolean getBit(byte[] ns, BitmapMetadata metadata, int bitIndex) {

        int fragmentIndex = bitIndex / fragmentSize;
        int fragmentBitIndex = bitIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            return false;
        }

        var bitSet = getRoaringBitmap(valueBytes);
        return bitSet.contains(fragmentBitIndex);
    }

    //Get all the bits set in the bitmap in all fragments
    public RoaringBitmap getAllBits(byte[] ns) {
        var metadata = getMetadata(ns).orElseThrow();
        return getAllBits(ns, metadata);
    }

    @SneakyThrows
    private RoaringBitmap getAllBits(byte[] ns, BitmapMetadata metadata) {
        RoaringBitmap roaringBitmap = null;
        for (int i = 0; i <= metadata.getMaxFragmentIndex(); i++) {
            byte[] keyBytes = getKey(metadata, ns, i);
            byte[] valueBytes = get(keyBytes);
            RoaringBitmap fragmentBitSet = null;
            if (valueBytes == null || valueBytes.length == 0) {
                fragmentBitSet = new RoaringBitmap();
            } else {
                fragmentBitSet = getRoaringBitmap(valueBytes);
            }

            fragmentBitSet = RoaringBitmap.addOffset(fragmentBitSet, i * fragmentSize);

            if (roaringBitmap == null) {
                roaringBitmap = fragmentBitSet;
            } else {
                roaringBitmap.or(fragmentBitSet);
            }
        }

        return roaringBitmap;
    }

    public RoaringBitmap getBits(byte[] ns, int fromFragmentIndex, int toFragmentIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return getBits(ns, metadata, fromFragmentIndex, toFragmentIndex);
    }

    @SneakyThrows
    private RoaringBitmap getBits(byte[] ns, BitmapMetadata metadata, int fromFragmentIndex, int toFragmentIndex) {
        RoaringBitmap roaringBitmap = null;
        for (int i = fromFragmentIndex; i <= toFragmentIndex; i++) {
            byte[] keyBytes = getKey(metadata, ns, i);
            byte[] valueBytes = get(keyBytes);
            RoaringBitmap fragmentBitSet = null;
            if (valueBytes == null || valueBytes.length == 0) {
                fragmentBitSet = new RoaringBitmap();
            } else {
                fragmentBitSet = getRoaringBitmap(valueBytes);
            }

            fragmentBitSet = RoaringBitmap.addOffset(fragmentBitSet, i * fragmentSize);

            if (roaringBitmap == null) {
                roaringBitmap = fragmentBitSet;
            } else {
                roaringBitmap.or(fragmentBitSet);
            }
        }

        return roaringBitmap;
    }

    public long nextSetBit(byte[] ns, int fromIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return nextSetBit(ns, metadata, fromIndex);
    }

    @SneakyThrows
    private long nextSetBit(byte[] ns, BitmapMetadata metadata, int fromIndex) {
        int fragmentIndex = fromIndex / fragmentSize;
        int fragmentBitIndex = fromIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        byte[] valueBytes = get(keyBytes);
        RoaringBitmap bitSet = null;
        if (valueBytes == null || valueBytes.length == 0) {
            bitSet = new RoaringBitmap();
        } else {
            bitSet = getRoaringBitmap(valueBytes);
        }

        long nextSetBit = bitSet.nextValue(fragmentBitIndex);
        if (nextSetBit == -1) {
            for (int i = fragmentIndex + 1; i <= metadata.getMaxFragmentIndex(); i++) {
                byte[] nextKeyBytes = getKey(metadata, ns, i);
                byte[] nextValueBytes = get(nextKeyBytes);
                if (nextValueBytes == null || nextValueBytes.length == 0) {
                    continue;
                }

                var nextBitSet = getRoaringBitmap(nextValueBytes);
                nextSetBit = nextBitSet.nextValue(0);
                if (nextSetBit != -1) {
                    return nextSetBit + (i * fragmentSize);
                }
            }
        } else {
            return nextSetBit + (fragmentIndex * fragmentSize);
        }

        return -1;
    }

    public long nextClearBit(byte[] ns, int fromIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return nextClearBit(ns, metadata, fromIndex);
    }

    @SneakyThrows
    private long nextClearBit(byte[] ns, BitmapMetadata metadata, int fromIndex) {
        int fragmentIndex = fromIndex / fragmentSize;
        int fragmentBitIndex = fromIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        RoaringBitmap bitSet = null;
        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            bitSet = new RoaringBitmap();
        } else {
            bitSet = getRoaringBitmap(valueBytes);
        }

        long nextClearBit = bitSet.nextAbsentValue(fragmentBitIndex);
        if (nextClearBit == -1) {
            for (int i = fragmentIndex + 1; i <= metadata.getMaxFragmentIndex(); i++) {
                byte[] nextKeyBytes = getKey(metadata, ns, i);
                byte[] nextValueBytes = get(nextKeyBytes);
                if (nextValueBytes == null || nextValueBytes.length == 0) {
                    continue;
                }

                var nextBitSet = getRoaringBitmap(nextValueBytes);
                nextClearBit = nextBitSet.nextAbsentValue(0);
                if (nextClearBit != -1) {
                    return nextClearBit + (i * fragmentSize);
                }
            }
        } else {
            return nextClearBit + (fragmentIndex * fragmentSize);
        }

        return -1;
    }

    public long previousSetBit(byte[] ns, int fromIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return previousSetBit(ns, metadata, fromIndex);
    }

    @SneakyThrows
    private long previousSetBit(byte[] ns, BitmapMetadata metadata, int fromIndex) {
        int fragmentIndex = fromIndex / fragmentSize;
        int fragmentBitIndex = fromIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        RoaringBitmap bitSet = null;
        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            bitSet = new RoaringBitmap();
        } else {
            bitSet = getRoaringBitmap(valueBytes);
        }

        long previousSetBit = bitSet.previousValue(fragmentBitIndex);
        if (previousSetBit == -1) {
            for (int i = fragmentIndex - 1; i >= 0; i--) {
                byte[] nextKeyBytes = getKey(metadata, ns, i);
                byte[] nextValueBytes = get(nextKeyBytes);
                RoaringBitmap nextBitSet = null;
                if (nextValueBytes == null || nextValueBytes.length == 0) {
                    nextBitSet = new RoaringBitmap();
                } else {
                    nextBitSet = getRoaringBitmap(nextValueBytes);
                }

                previousSetBit = nextBitSet.previousValue(fragmentSize - 1);
                if (previousSetBit != -1) {
                    return previousSetBit + (i * fragmentSize);
                }
            }
        } else {
            return previousSetBit + (fragmentIndex * fragmentSize);
        }

        return -1;
    }

    public long previousClearBit(byte[] ns, int fromIndex) {
        var metadata = getMetadata(ns).orElseThrow();
        return previousClearBit(ns, metadata, fromIndex);
    }

    @SneakyThrows
    private long previousClearBit(byte[] ns, BitmapMetadata metadata, int fromIndex) {
        int fragmentIndex = fromIndex / fragmentSize;
        int fragmentBitIndex = fromIndex % fragmentSize;

        byte[] keyBytes = getKey(metadata, ns, fragmentIndex);

        RoaringBitmap bitSet = null;
        byte[] valueBytes = get(keyBytes);
        if (valueBytes == null || valueBytes.length == 0) {
            bitSet = new RoaringBitmap();
        } else {
            bitSet = getRoaringBitmap(valueBytes);
        }

        long previousClearBit = bitSet.previousAbsentValue(fragmentBitIndex);
        if (previousClearBit == -1) {
            for (int i = fragmentIndex - 1; i >= 0; i--) {
                byte[] nextKeyBytes = getKey(metadata, ns, i);
                byte[] nextValueBytes = get(nextKeyBytes);
                RoaringBitmap nextBitSet = null;
                if (nextValueBytes == null || nextValueBytes.length == 0) {
                    nextBitSet = new RoaringBitmap();
                } else {
                    nextBitSet = getRoaringBitmap(nextValueBytes);
                }

                previousClearBit = nextBitSet.previousAbsentValue(fragmentSize - 1);
                if (previousClearBit != -1) {
                    return previousClearBit + (i * fragmentSize);
                }
            }
        } else {
            return previousClearBit + (fragmentIndex * fragmentSize);
        }

        return -1;
    }

    @SneakyThrows
    private BitmapMetadata updateMetadata(WriteBatch writeBatch, BitmapMetadata metadata, byte[] ns) {
        var metadataKeyName = getMetadataKey(ns);
        write(writeBatch, metadataKeyName, valueSerializer.serialize(metadata));
        return metadata;
    }

    @SneakyThrows
    protected Optional<BitmapMetadata> getMetadata(byte[] ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadataValueBytes = get(metadataKeyName);
        if (metadataValueBytes == null || metadataValueBytes.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(valueSerializer.deserialize(metadataValueBytes, BitmapMetadata.class));
        }
    }

    @Override
    protected Optional<BitmapMetadata> createMetadata(byte[] ns) {
        byte[] metadataKeyName = getMetadataKey(ns);
        var metadata = getMetadata(ns);
        if (metadata.isEmpty()) {
            var newMetadata = new BitmapMetadata();
            newMetadata.setVersion(System.currentTimeMillis());
            write(null, metadataKeyName, valueSerializer.serialize(newMetadata));
            return Optional.of(newMetadata);
        } else {
            return metadata;
        }
    }

    protected byte[] getMetadataKey(byte[] ns) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .build();
        else
            return new KeyBuilder(name)
                    .build();
    }

    private byte[] getKey(BitmapMetadata metadata, byte[] ns, int fragmentIndex) {
        if (ns != null)
            return new KeyBuilder(name, ns)
                    .append(metadata.getVersion())
                    .append(fragmentIndex)
                    .build();
        else
            return new KeyBuilder(name)
                    .append(metadata.getVersion())
                    .append(fragmentIndex)
                    .build();
    }
}
