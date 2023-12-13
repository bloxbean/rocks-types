package com.bloxbean.rocks.types.config;

import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class RocksDBProperties {
    private String columnFamilies;
    private String rocksDBBaseDir;

    public List<String> getColumnFamilyNames() {
        return Arrays.asList(columnFamilies.split(","));
    }

    public String getRocksDBBaseDir() {
        return rocksDBBaseDir;
    }
}
