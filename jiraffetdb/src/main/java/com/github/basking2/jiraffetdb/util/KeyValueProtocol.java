package com.github.basking2.jiraffetdb.util;

import java.nio.ByteBuffer;

/**
 * Class for reading and writing key/value pairs to a {@link ByteBuffer}.
 */
public class KeyValueProtocol {
    public static ByteBuffer marshal(final String key, final byte[] value) {
        final int len = key.getBytes().length + value.length + 8;
        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(key.getBytes().length);
        bb.put(key.getBytes());
        bb.putInt(value.length);
        bb.put(value);

        return bb;
    }

    public static KeyValue unmarshal(final ByteBuffer bb) {
        final byte[] nameba = getField(bb);
        final byte[] valueba = getField(bb);

        return new KeyValue(new String(nameba), valueba);
    }

    private static byte[] getField(final ByteBuffer bb) {
        final int len = bb.getInt();
        if (len < 0) {
            throw new IllegalArgumentException("Field length at index "+(bb.position()-4)+" may not be negative value "+len);
        }

        final byte[] field = new byte[len];
        bb.get(field);

        return field;
    }
}
