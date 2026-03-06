package com.sequoiadb.driver.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class MsgOpKillContexts {
    public MsgHeader header;
    public List<Long> contextIds = new ArrayList<>();

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE + 8 + contextIds.size() * 8)
                .order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI32(buf, 0); // ZERO
        Codec.writeI32(buf, contextIds.size());
        for (long id : contextIds) {
            Codec.writeI64(buf, id);
        }

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }

    public static MsgOpKillContexts decode(MsgHeader header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpKillContexts msg = new MsgOpKillContexts();
        msg.header = header;
        Codec.readI32(buf); // ZERO
        int numContexts = Codec.readI32(buf);
        for (int i = 0; i < numContexts; i++) {
            msg.contextIds.add(Codec.readI64(buf));
        }
        return msg;
    }
}
