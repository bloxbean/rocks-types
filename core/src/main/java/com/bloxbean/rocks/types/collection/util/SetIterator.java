package com.bloxbean.rocks.types.collection.util;

import com.bloxbean.rocks.types.common.KeyBuilder;
import com.bloxbean.rocks.types.serializer.Serializer;
import lombok.NonNull;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;

public class SetIterator<T> implements ValueIterator<T> {
    private final RocksIterator iterator;
    private final byte[] prefix;
    private final Serializer valueSerializer;
    private final Class<T> valueType;

    public SetIterator(@NonNull RocksIterator rocksIterator,
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
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        byte[] key = iterator.key();
        iterator.next();
        return valueSerializer.deserialize(KeyBuilder.removePrefix(key, prefix), valueType);
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
