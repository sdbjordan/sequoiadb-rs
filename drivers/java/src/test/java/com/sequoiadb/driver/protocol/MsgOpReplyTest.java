package com.sequoiadb.driver.protocol;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MsgOpReplyTest {

    @Test
    void emptyReplyRoundtrip() {
        MsgOpReply reply = new MsgOpReply();
        reply.header = MsgHeader.newRequest(OpCode.QUERY_REQ | OpCode.REPLY_MASK, 1);
        reply.contextId = -1;
        reply.flags = 0;
        reply.numReturned = 0;

        byte[] data = reply.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpReply decoded = MsgOpReply.decode(header, payload);
        assertEquals(0, decoded.flags);
        assertEquals(0, decoded.numReturned);
        assertTrue(decoded.docs.isEmpty());
    }

    @Test
    void replyWithDocsRoundtrip() {
        MsgOpReply reply = new MsgOpReply();
        reply.header = MsgHeader.newRequest(OpCode.QUERY_REQ | OpCode.REPLY_MASK, 1);
        reply.contextId = -1;
        reply.flags = 0;
        reply.numReturned = 2;
        reply.docs = List.of(new Document("a", 1), new Document("b", "hello"));

        byte[] data = reply.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpReply decoded = MsgOpReply.decode(header, payload);
        assertEquals(2, decoded.numReturned);
        assertEquals(2, decoded.docs.size());
        assertEquals(1, decoded.docs.get(0).getInteger("a"));
        assertEquals("hello", decoded.docs.get(1).getString("b"));
    }

    @Test
    void errorReply() {
        MsgOpReply reply = new MsgOpReply();
        reply.header = MsgHeader.newRequest(OpCode.INSERT_REQ | OpCode.REPLY_MASK, 42);
        reply.contextId = -1;
        reply.flags = -38; // DuplicateKey
        reply.numReturned = 0;

        byte[] data = reply.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpReply decoded = MsgOpReply.decode(header, payload);
        assertTrue(decoded.flags < 0);
        assertTrue(decoded.docs.isEmpty());
    }
}
