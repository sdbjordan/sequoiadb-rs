package com.sequoiadb.driver.protocol;

import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MsgOpQuery {
    public MsgHeader header;
    public int version = 1;
    public short w = 1;
    public int flags = 0;
    public int nameLength = 0;
    public long numToSkip = 0;
    public long numToReturn = -1;
    public String name = "";
    public Document condition;
    public Document selector;
    public Document orderBy;
    public Document hint;

    public static MsgOpQuery create(long requestId, String collection,
                                     Document condition, Document selector,
                                     Document orderBy, Document hint,
                                     long skip, long limit, int flags) {
        MsgOpQuery q = new MsgOpQuery();
        q.header = MsgHeader.newRequest(OpCode.QUERY_REQ, requestId);
        q.version = 1;
        q.w = 1;
        q.flags = flags;
        q.nameLength = collection.getBytes().length + 1;
        q.numToSkip = skip;
        q.numToReturn = limit;
        q.name = collection;
        q.condition = condition;
        q.selector = selector;
        q.orderBy = orderBy;
        q.hint = hint;
        return q;
    }

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);
        header.encode(buf);
        Codec.writeI32(buf, version);
        Codec.writeI16(buf, w);
        Codec.writeI16(buf, (short) 0); // padding
        Codec.writeI32(buf, flags);
        Codec.writeI32(buf, nameLength);
        Codec.writeI64(buf, numToSkip);
        Codec.writeI64(buf, numToReturn);
        Codec.writeCString(buf, name);
        Codec.padAlign4(buf);

        writeOptionalDoc(buf, condition);
        writeOptionalDoc(buf, selector);
        writeOptionalDoc(buf, orderBy);
        writeOptionalDoc(buf, hint);

        int len = buf.position();
        buf.putInt(0, len);
        byte[] result = new byte[len];
        buf.flip();
        buf.get(result);
        return result;
    }

    public static MsgOpQuery decode(MsgHeader header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        MsgOpQuery q = new MsgOpQuery();
        q.header = header;
        q.version = Codec.readI32(buf);
        q.w = Codec.readI16(buf);
        Codec.readI16(buf); // padding
        q.flags = Codec.readI32(buf);
        q.nameLength = Codec.readI32(buf);
        q.numToSkip = Codec.readI64(buf);
        q.numToReturn = Codec.readI64(buf);
        q.name = Codec.readCString(buf);
        Codec.skipAlign4(buf);

        q.condition = readOptionalDoc(buf);
        q.selector = readOptionalDoc(buf);
        q.orderBy = readOptionalDoc(buf);
        q.hint = readOptionalDoc(buf);
        return q;
    }

    static void writeOptionalDoc(ByteBuffer buf, Document doc) {
        byte[] bson = BsonHelper.encode(doc != null ? doc : new Document());
        buf.put(bson);
    }

    static Document readOptionalDoc(ByteBuffer buf) {
        if (buf.remaining() < 5) return null;
        Document doc = BsonHelper.decode(buf);
        if (doc == null || doc.isEmpty()) return null;
        return doc;
    }
}
