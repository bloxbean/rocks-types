package com.bloxbean.rocks.types.collection.util;

import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.NonNull;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;

import static com.bloxbean.rocks.types.common.KeyBuilder.bytesToLong;
import static com.bloxbean.rocks.types.common.KeyBuilder.longToBytes;

public class ZSetRangeIterator<T> implements ValueIterator<Tuple<T, Long>> {
    private final RocksIterator iterator;
    private final byte[] prefixWithoutScore;
    private final long endScore;
    private final Serializer valueSerializer;
    private final Class<T> valueType;

    public ZSetRangeIterator(@NonNull RocksIterator iterator,
                             @NonNull byte[] prefixWithoutScore,
                             long beginningScore,
                             long endScore,
                             @NonNull Serializer valueSerializer,
                             @NonNull Class<T> valueType) {
        this.iterator = iterator;
        this.prefixWithoutScore = prefixWithoutScore;
        this.endScore = endScore;
        this.valueSerializer = valueSerializer;
        this.valueType = valueType;
        byte[] prefix = KeyBuilder.appendToKey(prefixWithoutScore, longToBytes(beginningScore));
        this.iterator.seek(prefix);
    }

    @Override
    public boolean hasNext() {
        if (!iterator.isValid() || !KeyBuilder.hasPrefix(iterator.key(), prefixWithoutScore)) {
            return false;
        }
        var keyWithoutPrefix = KeyBuilder.removePrefix(iterator.key(), prefixWithoutScore);
        var parts = KeyBuilder.parts(keyWithoutPrefix);
        long score = bytesToLong(parts.get(0));
        return score <= endScore;
    }

    @Override
    public Tuple<T, Long> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        byte[] key = iterator.key();
        iterator.next();

        var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefixWithoutScore);
        var parts = KeyBuilder.parts(keyWithoutPrefix);
        long score = bytesToLong(parts.get(0));
        T member = valueSerializer.deserialize(parts.get(1), valueType);

        return new Tuple<>(member, score);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }

    @Override
    public void close() {
        iterator.close();
    }
}
