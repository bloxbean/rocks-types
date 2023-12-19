package com.bloxbean.rocks.types.collection.util;

import java.util.NoSuchElementException;

public class EmptyIterator<T> implements ValueIterator<T> {

    public EmptyIterator() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }

    @Override
    public void close() {
    }
}

