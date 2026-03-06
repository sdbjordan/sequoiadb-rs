package com.sequoiadb.driver.protocol;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class CodecTest {

    @Test
    void i32Roundtrip() {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeI32(buf, -12345);
        buf.flip();
        assertEquals(-12345, Codec.readI32(buf));
    }

    @Test
    void i64Roundtrip() {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeI64(buf, 0x7FFFFFFFFFFFFFFFL);
        buf.flip();
        assertEquals(0x7FFFFFFFFFFFFFFFL, Codec.readI64(buf));
    }

    @Test
    void i16Roundtrip() {
        ByteBuffer buf = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeI16(buf, (short) -300);
        buf.flip();
        assertEquals((short) -300, Codec.readI16(buf));
    }

    @Test
    void cstringRoundtrip() {
        ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeCString(buf, "hello");
        buf.flip();
        assertEquals("hello", Codec.readCString(buf));
        assertEquals(6, buf.position()); // 5 chars + null
    }

    @Test
    void padAlign4() {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buf.position(5);
        Codec.padAlign4(buf);
        assertEquals(8, buf.position());

        ByteBuffer buf2 = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buf2.position(4);
        Codec.padAlign4(buf2);
        assertEquals(4, buf2.position()); // already aligned
    }

    @Test
    void emptyCstring() {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeCString(buf, "");
        buf.flip();
        assertEquals("", Codec.readCString(buf));
        assertEquals(1, buf.position()); // just the null byte
    }
}
