# rocks-types

Data Type Implementations Built on RocksDB

This library provides some basic data types implemented on top of RocksDB. The main goal is to provide a simple and efficient
way to store and retrieve data from RocksDB.

<b>This library is still in experimental stage. </b>

## Supported Types

- [x] **List (RocksList) :** A basic implementation for ordered list where you can add and retrieve items by index.
- [x] **MultiList (RocksMultiList) :** A basic implementation for ordered list where you can add and retrieve items by index. It supports multiple lists under the same name, but different namespaces.
- [x] **Set (RocksSet) :** A basic implementation for set where you can add, remove and check if an item exists.
- [x] **MultiSet (RocksMultiSet) :** A basic implementation for set where you can add, remove and check if an item exists. It supports multiple sets under the same name, but different namespaces.
- [x] **ZSet (RocksZSet) :** A basic implementation for sorted set where you can add, remove and check if an item exists. You can find score of an item and items within a score range.
- [x] **MultiZSet (RocksMultiZSet) :** A basic implementation for sorted set where you can add, remove and check if an item exists. You can find score of an item and items within a score range. It supports multiple sorted sets under the same name, but different namespaces.
- [x] **Map (RocksMap) :** A basic implementation for map where you can add, remove and check if an item exists.
- [x] **MultiMap (RocksMultiMap) :** A basic implementation for map where you can add, remove and check if an item exists. It supports multiple maps under the same name, but different namespaces.
- [x] **Bitmap (RocksBitmap) :** A basic implementation for bitmap where you can set, unset and check if a bit is set.
- [x] **MultiBitmap (RocksMultiBitmap) :** A basic implementation for bitmap where you can set, unset and check if a bit is set. It supports multiple bitmaps under the same name, but different namespaces.

## Pre-requisites

- Java 21+

## Usage

Please check the test cases for usage [examples](https://github.com/bloxbean/rocks-types/tree/main/core/src/test/java/com/bloxbean/rocks/types).

## Dependencies

```xml
    <dependency>
        <groupId>com.bloxbean</groupId>
        <artifactId>rocks-types</artifactId>
        <version>{version}</version>
    </dependency>
```

## SNAPSHOT Dependency

Add Snapshot repository

```xml
    <repositories>
        <repository>
            <id>snapshots-repo</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
            <releases>
                <enabled>false</enabled>
            </releases>
            <snapshots>
                <enabled>true</enabled>
            </snapshots>
        </repository>
    </repositories>
```

## Build

```
./gradlew clean build
```

## License

MIT
