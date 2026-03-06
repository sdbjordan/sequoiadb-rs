package com.sequoiadb.driver.protocol;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class MsgHeaderTest {

    @Test
    void sizeIs52() {
        assertEquals(52, MsgHeader.SIZE);
    }

    @Test
    void roundtrip() {
        MsgHeader h = new MsgHeader();
        h.msgLen = 100;
        h.eye = MsgHeader.EYE_DEFAULT;
        h.tid = 42;
        h.routeId = 0xDEADBEEFL;
        h.requestId = 12345;
        h.opcode = 2004;
        h.version = 1;
        h.flags = 0;
        h.queryId = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        h.queryOpId = 99;

        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        h.encode(buf);
        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);
        assertEquals(MsgHeader.SIZE, data.length);

        MsgHeader decoded = MsgHeader.decode(data);
        assertEquals(100, decoded.msgLen);
        assertEquals(MsgHeader.EYE_DEFAULT, decoded.eye);
        assertEquals(42, decoded.tid);
        assertEquals(0xDEADBEEFL, decoded.routeId);
        assertEquals(12345, decoded.requestId);
        assertEquals(2004, decoded.opcode);
        assertEquals(1, decoded.version);
        assertEquals(0, decoded.flags);
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, decoded.queryId);
        assertEquals(99, decoded.queryOpId);
    }

    @Test
    void newRequestDefaults() {
        MsgHeader h = MsgHeader.newRequest(2004, 1);
        assertEquals(MsgHeader.SIZE, h.msgLen);
        assertEquals(MsgHeader.EYE_DEFAULT, h.eye);
        assertEquals(2004, h.opcode);
        assertEquals(1, h.requestId);
        assertEquals(1, h.version);
        assertEquals(0, h.flags);
    }

    @Test
    void decodeTooShort() {
        assertThrows(IllegalArgumentException.class, () -> MsgHeader.decode(new byte[40]));
    }
}
