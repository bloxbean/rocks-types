package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class SetMetadata extends TypeMetadata {
    private int size;

    @Override
    public DataType getType() {
        return DataType.SET;
    }
}
