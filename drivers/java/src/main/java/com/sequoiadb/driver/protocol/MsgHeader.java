package com.sequoiadb.driver.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 52-byte message header for SequoiaDB wire protocol.
 */
public class MsgHeader {
    public static final int SIZE = 52;
    public static final int EYE_DEFAULT = 0x0000EEEE;

    public int msgLen;
    public int eye;
    public int tid;
    public long routeId;
    public long requestId;
    public int opcode;
    public short version;
    public short flags;
    public byte[] queryId;
    public int queryOpId;
    public byte[] reserve;

    public MsgHeader() {
        this.msgLen = SIZE;
        this.eye = EYE_DEFAULT;
        this.queryId = new byte[8];
        this.reserve = new byte[4];
        this.version = 1;
    }

    public static MsgHeader newRequest(int opcode, long requestId) {
        MsgHeader h = new MsgHeader();
        h.opcode = opcode;
        h.requestId = requestId;
        return h;
    }

    public void encode(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        Codec.writeI32(buf, msgLen);
        Codec.writeI32(buf, eye);
        Codec.writeU32(buf, tid);
        Codec.writeU64(buf, routeId);
        Codec.writeU64(buf, requestId);
        Codec.writeI32(buf, opcode);
        Codec.writeI16(buf, version);
        Codec.writeI16(buf, flags);
        buf.put(queryId, 0, 8);
        Codec.writeU32(buf, queryOpId);
        buf.put(reserve, 0, 4);
    }

    public static MsgHeader decode(byte[] data) {
        if (data.length < SIZE) {
            throw new IllegalArgumentException("Header too short: " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        MsgHeader h = new MsgHeader();
        h.msgLen = Codec.readI32(buf);
        h.eye = Codec.readI32(buf);
        h.tid = Codec.readU32(buf);
        h.routeId = Codec.readU64(buf);
        h.requestId = Codec.readU64(buf);
        h.opcode = Codec.readI32(buf);
        h.version = Codec.readI16(buf);
        h.flags = Codec.readI16(buf);
        h.queryId = Codec.readBytes(buf, 8);
        h.queryOpId = Codec.readU32(buf);
        h.reserve = Codec.readBytes(buf, 4);
        return h;
    }
}
