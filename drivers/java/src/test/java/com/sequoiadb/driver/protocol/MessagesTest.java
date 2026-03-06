package com.sequoiadb.driver.protocol;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MessagesTest {

    @Test
    void queryEncodeDecodeRoundtrip() {
        MsgOpQuery q = MsgOpQuery.create(1, "mycs.mycl",
                new Document("x", 1), null, null, null, 10, 100, 0);
        byte[] data = q.encode();
        MsgHeader header = MsgHeader.decode(data);
        assertEquals(OpCode.QUERY_REQ, header.opcode);
        assertEquals(data.length, header.msgLen);

        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpQuery decoded = MsgOpQuery.decode(header, payload);
        assertEquals("mycs.mycl", decoded.name);
        assertEquals(10, decoded.numToSkip);
        assertEquals(100, decoded.numToReturn);
        assertNotNull(decoded.condition);
        assertNull(decoded.selector);
    }

    @Test
    void queryCommandRoundtrip() {
        MsgOpQuery q = MsgOpQuery.create(2, "$create collectionspace",
                new Document("Name", "mycs"), null, null, null, 0, -1, 0);
        byte[] data = q.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpQuery decoded = MsgOpQuery.decode(header, payload);
        assertEquals("$create collectionspace", decoded.name);
    }

    @Test
    void insertSingleDocRoundtrip() {
        MsgOpInsert msg = MsgOpInsert.create(1, "cs.cl",
                List.of(new Document("a", 42)), 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpInsert decoded = MsgOpInsert.decode(header, payload);
        assertEquals("cs.cl", decoded.name);
        assertEquals(1, decoded.docs.size());
        assertEquals(42, decoded.docs.get(0).getInteger("a"));
    }

    @Test
    void insertMultiDocRoundtrip() {
        List<Document> docs = List.of(
                new Document("x", 1),
                new Document("x", 2),
                new Document("x", 3)
        );
        MsgOpInsert msg = MsgOpInsert.create(1, "cs.cl", docs, 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpInsert decoded = MsgOpInsert.decode(header, payload);
        assertEquals(3, decoded.docs.size());
    }

    @Test
    void updateRoundtrip() {
        MsgOpUpdate msg = MsgOpUpdate.create(1, "cs.cl",
                new Document("x", 1), new Document("$set", new Document("x", 2)), null, 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpUpdate decoded = MsgOpUpdate.decode(header, payload);
        assertEquals("cs.cl", decoded.name);
        assertEquals(1, decoded.condition.getInteger("x"));
    }

    @Test
    void updateWithHint() {
        MsgOpUpdate msg = MsgOpUpdate.create(1, "cs.cl",
                new Document("x", 1), new Document("$set", new Document("x", 2)),
                new Document("", "idx"), 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpUpdate decoded = MsgOpUpdate.decode(header, payload);
        assertNotNull(decoded.hint);
    }

    @Test
    void deleteRoundtrip() {
        MsgOpDelete msg = MsgOpDelete.create(1, "cs.cl",
                new Document("x", 1), null, 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpDelete decoded = MsgOpDelete.decode(header, payload);
        assertEquals("cs.cl", decoded.name);
        assertEquals(1, decoded.condition.getInteger("x"));
    }

    @Test
    void deleteWithHint() {
        MsgOpDelete msg = MsgOpDelete.create(1, "cs.cl",
                new Document("x", 1), new Document("", "idx"), 0);
        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpDelete decoded = MsgOpDelete.decode(header, payload);
        assertNotNull(decoded.hint);
    }

    @Test
    void getMoreRoundtrip() {
        MsgOpGetMore msg = new MsgOpGetMore();
        msg.header = MsgHeader.newRequest(OpCode.GET_MORE_REQ, 1);
        msg.contextId = 42;
        msg.numToReturn = 100;

        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpGetMore decoded = MsgOpGetMore.decode(header, payload);
        assertEquals(42, decoded.contextId);
        assertEquals(100, decoded.numToReturn);
    }

    @Test
    void killContextRoundtrip() {
        MsgOpKillContexts msg = new MsgOpKillContexts();
        msg.header = MsgHeader.newRequest(OpCode.KILL_CONTEXT_REQ, 1);
        msg.contextIds = List.of(10L, 20L, 30L);

        byte[] data = msg.encode();
        MsgHeader header = MsgHeader.decode(data);
        byte[] payload = Arrays.copyOfRange(data, MsgHeader.SIZE, data.length);
        MsgOpKillContexts decoded = MsgOpKillContexts.decode(header, payload);
        assertEquals(List.of(10L, 20L, 30L), decoded.contextIds);
    }
}
