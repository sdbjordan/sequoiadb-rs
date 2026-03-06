package com.sequoiadb.driver;

import com.sequoiadb.driver.protocol.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DBCollection {
    final SdbClient client;
    final String fullName;

    public DBCollection(SdbClient client, String fullName) {
        this.client = client;
        this.fullName = fullName;
    }

    public String getFullName() { return fullName; }

    public void insert(Document doc) {
        insertMany(Collections.singletonList(doc));
    }

    public void insertMany(List<Document> docs) {
        MsgOpReply reply = client.sendRecv(rid ->
            MsgOpInsert.create(rid, fullName, docs, 0).encode()
        );
        SdbClient.checkReply(reply);
    }

    public void insertBatch(List<Document> docs) {
        insertMany(docs);
    }

    public DBCursor query(Document condition) {
        MsgOpReply reply = client.sendRecv(rid ->
            MsgOpQuery.create(rid, fullName, condition, null, null, null, 0, -1, 0).encode()
        );
        SdbClient.checkReply(reply);
        return new DBCursor(reply.docs, reply.contextId, client);
    }

    public DBCursor query() {
        return query(null);
    }

    public void update(Document condition, Document modifier) {
        MsgOpReply reply = client.sendRecv(rid ->
            MsgOpUpdate.create(rid, fullName, condition, modifier, null, 0).encode()
        );
        SdbClient.checkReply(reply);
    }

    public void delete(Document condition) {
        MsgOpReply reply = client.sendRecv(rid ->
            MsgOpDelete.create(rid, fullName, condition, null, 0).encode()
        );
        SdbClient.checkReply(reply);
    }

    public long count(Document condition) {
        MsgOpReply reply = client.sendRecv(rid -> {
            Document cond = new Document("Collection", fullName);
            if (condition != null) {
                cond.append("Condition", condition);
            }
            return MsgOpQuery.create(rid, "$count", cond, null, null, null, 0, -1, 0).encode();
        });
        SdbClient.checkReply(reply);
        if (!reply.docs.isEmpty()) {
            Object c = reply.docs.get(0).get("count");
            if (c instanceof Number) return ((Number) c).longValue();
        }
        return 0;
    }

    public long count() {
        return count(null);
    }

    public List<Document> aggregate(List<Document> pipeline) {
        MsgOpReply reply = client.sendRecv(rid -> {
            Document cond = new Document("Collection", fullName)
                    .append("Pipeline", pipeline);
            return MsgOpQuery.create(rid, "$aggregate", cond, null, null, null, 0, -1, 0).encode();
        });
        SdbClient.checkReply(reply);
        return reply.docs;
    }
}
