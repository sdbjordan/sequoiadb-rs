package com.sequoiadb.driver.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MsgOpGetMore {
    public MsgHeader header;
    public long contextId;
    public int numToReturn;

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE + 12).order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI64(buf, contextId);
        Codec.writeI32(buf, numToReturn);

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }

    public static MsgOpGetMore decode(MsgHeader header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpGetMore msg = new MsgOpGetMore();
        msg.header = header;
        msg.contextId = Codec.readI64(buf);
        msg.numToReturn = Codec.readI32(buf);
        return msg;
    }
}
