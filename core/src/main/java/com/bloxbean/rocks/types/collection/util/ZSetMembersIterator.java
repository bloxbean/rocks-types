package com.bloxbean.rocks.types.collection.util;

import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.common.Tuple;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.NonNull;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;

public class ZSetMembersIterator<T> implements ValueIterator<Tuple<T, Long>> {
    private final RocksIterator iterator;
    private final byte[] prefix;
    private final Serializer valueSerializer;
    private final Class<T> valueType;

    public ZSetMembersIterator(@NonNull RocksIterator rocksIterator,
                               @NonNull byte[] prefix,
                               @NonNull Serializer valueSerializer,
                               @NonNull Class<T> valueType) {
        this.iterator = rocksIterator;
        this.prefix = prefix;
        this.valueSerializer = valueSerializer;
        this.valueType = valueType;
        this.iterator.seek(prefix);
    }

    @Override
    public boolean hasNext() {
        return iterator.isValid() && KeyBuilder.hasPrefix(iterator.key(), prefix);
    }

    @Override
    public Tuple<T, Long> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        byte[] key = iterator.key();
        byte[] value = iterator.value();
        iterator.next();

        var keyWithoutPrefix = KeyBuilder.removePrefix(key, prefix);
        var parts = KeyBuilder.parts(keyWithoutPrefix);
        T member = valueSerializer.deserialize(parts.get(0), valueType);
        Long score = valueSerializer.deserialize(value, Long.class);

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
