package com.bloxbean.rocks.types.common;

import lombok.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class KeyBuilder {
    private static final byte SEPARATOR = ':';
    private String name;
    private String ns;
    private ByteBuffer byteBuffer;
    private List<byte[]> parts = new ArrayList<>();

    public KeyBuilder(@NonNull String name) {
        this.name = name;

        var nameBytes = name.getBytes(StandardCharsets.UTF_8);
        parts.add(nameBytes);
    }

    public KeyBuilder(@NonNull String name, @NonNull String ns) {
        this.name = name;
        this.ns = ns;

        var nameBytes = name.getBytes(StandardCharsets.UTF_8);
        var nsBytes = ns.getBytes(StandardCharsets.UTF_8);
        parts.add(nameBytes);
        parts.add(nsBytes);
    }

//    public KeyBuilder(byte[] key) {
//        var keyBuilder = KeyBuilder.fromKey(key);
//        this.parts = keyBuilder.parts;
//    }

    public byte[] build() {
        //merge name + ns + parts with prefix
        byte[] buffer = new byte[parts.stream().mapToInt(a -> a.length).sum() + parts.size() - 1];
        //give logic to merge byte arrays
        int start = 0;
        //loop through parts and merge with separator between each part
        //Don't add separator for last part
        int i = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, buffer, start, part.length);
            start += part.length;
            if (i < parts.size() - 1) {
                buffer[start] = SEPARATOR;
                start += 1;
            }
            i++;
        }

        return buffer;
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
        // Include the key, all parts, and a separator for each part (including one after the key)
        int totalLength = key.length + 1 + Arrays.stream(parts).mapToInt(a -> a.length).sum() + parts.length - 1;
        byte[] buffer = new byte[totalLength];

        // Copy the key to the buffer
        System.arraycopy(key, 0, buffer, 0, key.length);

        // Add a separator after the key
        int start = key.length;
        buffer[start++] = SEPARATOR;

        // Loop through parts and merge with separator between each part
        for (int i = 0; i < parts.length; i++) {
            byte[] part = parts[i];
            System.arraycopy(part, 0, buffer, start, part.length);
            start += part.length;

            // Add a separator after each part, except the last one
            if (i < parts.length - 1) {
                buffer[start++] = SEPARATOR;
            }
        }

        return buffer;
    }

    //split key into parts
    public static List<byte[]> parts(byte[] key) {
        List<byte[]> parts = new ArrayList<>();
        int start = 0;

        for (int i = 0; i < key.length; i++) {
            if (key[i] == SEPARATOR) {
                byte[] part = new byte[i - start];
                System.arraycopy(key, start, part, 0, part.length);
                parts.add(part);
                start = i + 1;
            }
        }

        // Add the last part if there's no separator at the end
        if (start < key.length) {
            byte[] part = new byte[key.length - start];
            System.arraycopy(key, start, part, 0, part.length);
            parts.add(part);
        }

        // If no separator found, return the original array in a list
        if (parts.isEmpty()) {
            parts.add(key);
        }

        return parts;
    }


    public KeyBuilder append(String str) {
        if (str != null) {
            parts.add(strToBytes(str));
            return this;
        } else {
            return this;
        }
    }

    public KeyBuilder append(byte[] bytes) {
        if (bytes == null)
            return this;
        parts.add(bytes);
        return this;
    }

    public KeyBuilder append(int value) {
        parts.add(intToBytes(value));
        return this;
    }

    public KeyBuilder append(long value) {
        parts.add(longToBytes(value));
        return this;
    }

    public KeyBuilder append(short value) {
        parts.add(shortToBytes(value));
        return this;
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

    //method to get sub array from key array by removing prefix and separator
    public static byte[] removePrefix(byte[] key, byte[] prefix) {
        if (key == null || prefix == null)
            return null;

        if (key.length < prefix.length)
            return null;

        byte[] subArray = new byte[key.length - prefix.length - 1];
        System.arraycopy(key, prefix.length + 1, subArray, 0, subArray.length);

        return subArray;
    }

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
