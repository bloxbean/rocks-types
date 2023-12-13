package com.bloxbean.rocks.types.collection.metadata;

public enum DataType {
    LIST((short) 0),
    SET((short) 1),
    MAP((short) 2);

    private final short value;
    DataType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }
}
