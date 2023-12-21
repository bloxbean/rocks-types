package com.bloxbean.rocks.types.collection.metadata;

public enum DataType {
    LIST((short) 0),
    SET((short) 1),
    MAP((short) 2),
    ZSET((short) 3),
    BITMAP((short) 4);

    private final short value;
    DataType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }
}
