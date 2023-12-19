package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class MapMetadata extends TypeMetadata {
    @Override
    public DataType getType() {
        return DataType.MAP;
    }
}
