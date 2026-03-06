package com.sequoiadb.driver.protocol;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Shared BSON encode/decode utilities.
 */
public final class BsonHelper {

    private static final DocumentCodec CODEC = new DocumentCodec();

    private BsonHelper() {}

    public static byte[] encode(Document doc) {
        BasicOutputBuffer out = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(out)) {
            CODEC.encode(writer, doc, EncoderContext.builder().build());
        }
        return out.toByteArray();
    }

    public static Document decode(ByteBuffer buf) {
        if (buf.remaining() < 5) return null;
        int pos = buf.position();
        int docLen = buf.order(ByteOrder.LITTLE_ENDIAN).getInt(pos);
        if (buf.remaining() < docLen) return null;

        byte[] docBytes = new byte[docLen];
        buf.get(docBytes);
        BsonBinaryReader reader = new BsonBinaryReader(
            ByteBuffer.wrap(docBytes).order(ByteOrder.LITTLE_ENDIAN)
        );
        return CODEC.decode(reader, DecoderContext.builder().build());
    }

    public static byte[] emptyDoc() {
        return encode(new Document());
    }
}
