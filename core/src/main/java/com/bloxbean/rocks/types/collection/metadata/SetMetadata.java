package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class SetMetadata extends TypeMetadata {
    @Override
    public DataType getType() {
        return DataType.SET;
    }
}
