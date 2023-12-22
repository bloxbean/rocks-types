package com.bloxbean.rocks.types.collection.util;

public interface ReverseValueIterator<T> extends AutoCloseable {
    boolean hasPrev();
    T prev();
}
