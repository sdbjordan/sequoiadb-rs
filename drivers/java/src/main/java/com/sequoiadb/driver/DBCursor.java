package com.sequoiadb.driver;

import com.sequoiadb.driver.protocol.*;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DBCursor {
    private List<Document> buffer;
    private int pos = 0;
    private boolean closed = false;
    private long contextId;
    private final SdbClient client;

    public DBCursor(List<Document> docs, long contextId, SdbClient client) {
        this.buffer = new ArrayList<>(docs);
        this.contextId = contextId;
        this.client = client;
    }

    public Document next() {
        if (closed || pos >= buffer.size()) return null;
        return buffer.get(pos++);
    }

    public boolean hasMore() {
        return !closed && contextId != -1;
    }

    public boolean fetchMore() {
        if (closed || contextId == -1 || client == null) return false;

        long ctxId = this.contextId;
        MsgOpReply reply = client.sendRecv(rid -> {
            MsgOpGetMore msg = new MsgOpGetMore();
            msg.header = MsgHeader.newRequest(OpCode.GET_MORE_REQ, rid);
            msg.contextId = ctxId;
            msg.numToReturn = -1;
            return msg.encode();
        });
        SdbClient.checkReply(reply);

        this.contextId = reply.contextId;
        this.buffer = new ArrayList<>(reply.docs);
        this.pos = 0;

        if (buffer.isEmpty()) {
            closed = true;
            return false;
        }
        return true;
    }

    public List<Document> collectAll() {
        List<Document> result = new ArrayList<>();
        while (true) {
            Document doc;
            while ((doc = next()) != null) {
                result.add(doc);
            }
            if (!fetchMore()) break;
        }
        return result;
    }

    public void close() {
        if (closed) return;
        closed = true;

        if (contextId != -1 && client != null) {
            long ctxId = this.contextId;
            try {
                client.sendRecv(rid -> {
                    MsgOpKillContexts msg = new MsgOpKillContexts();
                    msg.header = MsgHeader.newRequest(OpCode.KILL_CONTEXT_REQ, rid);
                    msg.contextIds = Collections.singletonList(ctxId);
                    return msg.encode();
                });
            } catch (Exception ignored) {}
            this.contextId = -1;
        }
    }
}
