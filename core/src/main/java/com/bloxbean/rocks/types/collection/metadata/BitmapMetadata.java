package com.bloxbean.rocks.types.collection.metadata;

import lombok.Data;

@Data
public class BitmapMetadata extends TypeMetadata {
    private long maxFragmentIndex;
    @Override
    public DataType getType() {
        return DataType.BITMAP;
    }
}
