package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class ListMetadata extends TypeMetadata {
    private long size;
    private byte[] head;
    private byte[] tail;

    public DataType getType() {
        return DataType.LIST;
    }
}
