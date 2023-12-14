package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class ZSetMetadata extends TypeMetadata {
    @Override
    public DataType getType() {
        return DataType.ZSET;
    }
}
