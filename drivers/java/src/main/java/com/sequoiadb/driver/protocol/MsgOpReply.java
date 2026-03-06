package com.sequoiadb.driver.protocol;

import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class MsgOpReply {
    public static final int FIXED_SIZE = 28;

    public MsgHeader header;
    public long contextId = -1;
    public int flags = 0;
    public int startFrom = 0;
    public int numReturned = 0;
    public int returnMask = 0;
    public int dataLen = 0;
    public List<Document> docs = new ArrayList<>();

    public static MsgOpReply decode(MsgHeader header, byte[] payload) {
        if (payload.length < FIXED_SIZE) {
            throw new IllegalArgumentException("Reply payload too short: " + payload.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpReply reply = new MsgOpReply();
        reply.header = header;
        reply.contextId = Codec.readI64(buf);
        reply.flags = Codec.readI32(buf);
        reply.startFrom = Codec.readI32(buf);
        reply.numReturned = Codec.readI32(buf);
        reply.returnMask = Codec.readI32(buf);
        reply.dataLen = Codec.readI32(buf);

        int dataEnd = buf.position() + reply.dataLen;
        while (buf.position() < dataEnd && buf.hasRemaining()) {
            Document doc = BsonHelper.decode(buf);
            if (doc != null) reply.docs.add(doc);
        }
        return reply;
    }

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(65536).order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI64(buf, contextId);
        Codec.writeI32(buf, flags);
        Codec.writeI32(buf, startFrom);
        Codec.writeI32(buf, numReturned);
        Codec.writeI32(buf, returnMask);

        // Encode docs
        byte[] docBytes;
        if (docs.isEmpty()) {
            docBytes = new byte[0];
        } else {
            ByteBuffer docBuf = ByteBuffer.allocate(65536).order(ByteOrder.LITTLE_ENDIAN);
            for (Document doc : docs) {
                docBuf.put(BsonHelper.encode(doc));
            }
            docBytes = new byte[docBuf.position()];
            docBuf.flip();
            docBuf.get(docBytes);
        }
        Codec.writeI32(buf, docBytes.length);
        buf.put(docBytes);

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }
}
