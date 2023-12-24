package com.bloxbean.rocks.types.common;

import lombok.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class KeyBuilder {
    private List<byte[]> parts = new ArrayList<>();

    public KeyBuilder(@NonNull String name) {
        addPart(name.getBytes(StandardCharsets.UTF_8));
    }

    public KeyBuilder(@NonNull byte[] name) {
        addPart(name);
    }

    public KeyBuilder(@NonNull String name, @NonNull String ns) {
        addPart(name.getBytes(StandardCharsets.UTF_8));
        addPart(ns.getBytes(StandardCharsets.UTF_8));
    }

    public KeyBuilder(@NonNull String name, @NonNull byte[] ns) {
        addPart(name.getBytes(StandardCharsets.UTF_8));
        addPart(ns);
    }

    public KeyBuilder(@NonNull byte[] name, @NonNull byte[] ns) {
        addPart(name);
        addPart(ns);
    }

    public byte[] build() {
        int totalLength = parts.stream().mapToInt(a -> Integer.BYTES + a.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        for (byte[] part : parts) {
            buffer.putInt(part.length);
            buffer.put(part);
        }

        return buffer.array();
    }

    private void addPart(byte[] part) {
        parts.add(part);
    }

    public KeyBuilder append(String str) {
        if (str != null) {
            addPart(str.getBytes(StandardCharsets.UTF_8));
        }
        return this;
    }

    public KeyBuilder append(byte[] bytes) {
        if (bytes != null) {
            addPart(bytes);
        }
        return this;
    }

    public KeyBuilder append(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        addPart(buffer.array());
        return this;
    }

    public KeyBuilder append(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        addPart(buffer.array());
        return this;
    }

    public KeyBuilder append(short value) {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(value);
        addPart(buffer.array());
        return this;
    }

    //check if key has prefix and prefix is a byte array
    public static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (key == null || prefix == null)
            return false;

        if (key.length < prefix.length)
            return false;

        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i])
                return false;
        }

        return true;
    }

    public static byte[] appendToKey(byte[] key, byte[]... parts) {
        // Calculate the total length needed for the buffer
        // Include the key and all parts, each prefixed with its length
        int totalLength = key.length + Arrays.stream(parts).mapToInt(a -> Integer.BYTES + a.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // Copy the key to the buffer
        buffer.put(key);

        // Loop through parts and add each with its length prefix
        for (byte[] part : parts) {
            buffer.putInt(part.length); // Prefix with length
            buffer.put(part);           // Add the part
        }

        return buffer.array();
    }


    public static byte[] intToBytes(final int i) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }

    public static byte[] longToBytes(final long i) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(i);
        return bb.array();
    }

    public static byte[] shortToBytes(final short i) {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.putShort(i);
        return bb.array();
    }

    public static byte[] strToBytes(final String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToStr(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static long bytesToLong(final byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(bytes);
        bb.flip();
        return bb.getLong();
    }

    public static int bytesToInt(final byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(bytes);
        bb.flip();
        return bb.getInt();
    }

    public static short bytesToShort(final byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.put(bytes);
        bb.flip();
        return bb.getShort();
    }

    public static List<byte[]> decodeCompositeKey(byte[] compositeKey) {
        List<byte[]> parts = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(compositeKey);

        while (buffer.hasRemaining()) {
            int length = buffer.getInt();
            byte[] part = new byte[length];
            buffer.get(part);
            parts.add(part);
        }

        return parts;
    }

    /**
    public static byte[] removePrefix(byte[] key, byte[] prefix) {
        if (key == null || prefix == null) {
            return null;
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        ByteBuffer prefixBuffer = ByteBuffer.wrap(prefix);

        while (prefixBuffer.hasRemaining() && keyBuffer.hasRemaining()) {
            int prefixPartLength = prefixBuffer.getInt();
            int keyPartLength = keyBuffer.getInt();

            if (prefixPartLength != keyPartLength) {
                // Lengths don't match, so this cannot be a prefix
                return null;
            }

            byte[] prefixPart = new byte[prefixPartLength];
            byte[] keyPart = new byte[keyPartLength];

            prefixBuffer.get(prefixPart);
            keyBuffer.get(keyPart);

            if (!Arrays.equals(prefixPart, keyPart)) {
                // Contents don't match, so this cannot be a prefix
                return null;
            }
        }

        // If we've consumed the entire prefix but there's still data in the key,
        // then the prefix is valid. Return the remaining part of the key.
        if (!prefixBuffer.hasRemaining() && keyBuffer.hasRemaining()) {
            byte[] remainingKey = new byte[keyBuffer.remaining()];
            keyBuffer.get(remainingKey);
            return remainingKey;
        }

        // If both buffers are empty, or the key buffer is empty (but not the prefix),
        // then the prefix is not valid.
        return null;
    }**/


    /**
     * Append two byte arrays
     *
     * @param a first byte array
     * @param b second byte array
     * @return bytes appended
     */
    public static byte[] merge(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    /**
     * Merge byte arrays.
     *
     * @param arrays
     * @return
     */
    public static byte[] merge(byte[]... arrays) {
        int total = Stream.of(arrays).mapToInt(a -> a.length).sum();

        byte[] buffer = new byte[total];
        int start = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, buffer, start, array.length);
            start += array.length;
        }

        return buffer;
    }


}
