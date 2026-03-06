package com.sequoiadb.driver.protocol;

import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class MsgOpInsert {
    public MsgHeader header;
    public int version = 1;
    public short w = 1;
    public int flags = 0;
    public int nameLength = 0;
    public String name = "";
    public List<Document> docs = new ArrayList<>();

    public static MsgOpInsert create(long requestId, String collection, List<Document> docs, int flags) {
        MsgOpInsert msg = new MsgOpInsert();
        msg.header = MsgHeader.newRequest(OpCode.INSERT_REQ, requestId);
        msg.version = 1;
        msg.w = 1;
        msg.flags = flags;
        msg.nameLength = collection.getBytes().length + 1;
        msg.name = collection;
        msg.docs = docs;
        return msg;
    }

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(65536).order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI32(buf, version);
        Codec.writeI16(buf, w);
        Codec.writeI16(buf, (short) 0);
        Codec.writeI32(buf, flags);
        Codec.writeI32(buf, nameLength);
        Codec.writeCString(buf, name);
        Codec.padAlign4(buf);

        for (Document doc : docs) {
            buf.put(BsonHelper.encode(doc));
        }

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }

    public static MsgOpInsert decode(MsgHeader header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpInsert msg = new MsgOpInsert();
        msg.header = header;
        msg.version = Codec.readI32(buf);
        msg.w = Codec.readI16(buf);
        Codec.readI16(buf);
        msg.flags = Codec.readI32(buf);
        msg.nameLength = Codec.readI32(buf);
        msg.name = Codec.readCString(buf);
        Codec.skipAlign4(buf);

        while (buf.hasRemaining()) {
            Document doc = BsonHelper.decode(buf);
            if (doc != null) msg.docs.add(doc);
        }
        return msg;
    }
}
