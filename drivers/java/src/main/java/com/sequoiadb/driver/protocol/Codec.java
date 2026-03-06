package com.sequoiadb.driver.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Low-level codec: LE read/write, C-string, 4-byte alignment.
 */
public final class Codec {

    private Codec() {}

    // ── Write ─────────────────────────────────────────────────────────

    public static void writeI32(ByteBuffer buf, int v) {
        buf.order(ByteOrder.LITTLE_ENDIAN).putInt(v);
    }

    public static void writeU32(ByteBuffer buf, int v) {
        buf.order(ByteOrder.LITTLE_ENDIAN).putInt(v);
    }

    public static void writeI16(ByteBuffer buf, short v) {
        buf.order(ByteOrder.LITTLE_ENDIAN).putShort(v);
    }

    public static void writeI64(ByteBuffer buf, long v) {
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(v);
    }

    public static void writeU64(ByteBuffer buf, long v) {
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(v);
    }

    public static void writeCString(ByteBuffer buf, String s) {
        buf.put(s.getBytes(StandardCharsets.UTF_8));
        buf.put((byte) 0);
    }

    public static void padAlign4(ByteBuffer buf) {
        int r = buf.position() % 4;
        if (r != 0) {
            for (int i = 0; i < 4 - r; i++) {
                buf.put((byte) 0);
            }
        }
    }

    // ── Read ──────────────────────────────────────────────────────────

    public static int readI32(ByteBuffer buf) {
        return buf.order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static int readU32(ByteBuffer buf) {
        return buf.order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static short readI16(ByteBuffer buf) {
        return buf.order(ByteOrder.LITTLE_ENDIAN).getShort();
    }

    public static long readI64(ByteBuffer buf) {
        return buf.order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public static long readU64(ByteBuffer buf) {
        return buf.order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public static String readCString(ByteBuffer buf) {
        int start = buf.position();
        while (buf.get() != 0) { /* scan for null */ }
        int end = buf.position() - 1; // exclude null
        byte[] bytes = new byte[end - start];
        buf.position(start);
        buf.get(bytes);
        buf.get(); // skip null
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(ByteBuffer buf, int n) {
        byte[] bytes = new byte[n];
        buf.get(bytes);
        return bytes;
    }

    public static void skipAlign4(ByteBuffer buf) {
        int r = buf.position() % 4;
        if (r != 0) {
            int pad = 4 - r;
            if (buf.position() + pad <= buf.limit()) {
                buf.position(buf.position() + pad);
            }
        }
    }
}
