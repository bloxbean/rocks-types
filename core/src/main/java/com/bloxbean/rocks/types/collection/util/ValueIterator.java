package com.bloxbean.rocks.types.collection.util;

import java.util.Iterator;

public interface ValueIterator<T> extends Iterator<T>, AutoCloseable {

    default void skip(int count) {
        int counter = 0;
        while (hasNext() && counter < count) {
            next();
            counter++;
        }
    }
}
