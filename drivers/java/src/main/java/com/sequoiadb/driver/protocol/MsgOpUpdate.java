package com.sequoiadb.driver.protocol;

import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MsgOpUpdate {
    public MsgHeader header;
    public int version = 1;
    public short w = 1;
    public int flags = 0;
    public int nameLength = 0;
    public String name = "";
    public Document condition = new Document();
    public Document modifier = new Document();
    public Document hint; // nullable

    public static MsgOpUpdate create(long requestId, String collection,
                                      Document condition, Document modifier,
                                      Document hint, int flags) {
        MsgOpUpdate msg = new MsgOpUpdate();
        msg.header = MsgHeader.newRequest(OpCode.UPDATE_REQ, requestId);
        msg.version = 1;
        msg.w = 1;
        msg.flags = flags;
        msg.nameLength = collection.getBytes().length + 1;
        msg.name = collection;
        msg.condition = condition;
        msg.modifier = modifier;
        msg.hint = hint;
        return msg;
    }

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI32(buf, version);
        Codec.writeI16(buf, w);
        Codec.writeI16(buf, (short) 0);
        Codec.writeI32(buf, flags);
        Codec.writeI32(buf, nameLength);
        Codec.writeCString(buf, name);
        Codec.padAlign4(buf);

        buf.put(BsonHelper.encode(condition));
        buf.put(BsonHelper.encode(modifier));
        if (hint != null) {
            buf.put(BsonHelper.encode(hint));
        }

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }

    public static MsgOpUpdate decode(MsgHeader header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpUpdate msg = new MsgOpUpdate();
        msg.header = header;
        msg.version = Codec.readI32(buf);
        msg.w = Codec.readI16(buf);
        Codec.readI16(buf);
        msg.flags = Codec.readI32(buf);
        msg.nameLength = Codec.readI32(buf);
        msg.name = Codec.readCString(buf);
        Codec.skipAlign4(buf);

        msg.condition = BsonHelper.decode(buf);
        msg.modifier = BsonHelper.decode(buf);
        if (buf.hasRemaining()) {
            msg.hint = BsonHelper.decode(buf);
        }
        return msg;
    }
}
