package com.sequoiadb.driver;

import org.bson.Document;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SdbClientTest {
    private static TestServer server;
    private static int port;

    @BeforeAll
    static void startServer() throws Exception {
        server = new TestServer();
        server.start();
        port = server.getPort();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) server.stop();
    }

    private SdbClient makeClient() {
        return new SdbClient("127.0.0.1", port);
    }

    // 1
    @Test @Order(1)
    void connectDisconnect() {
        SdbClient client = makeClient();
        assertTrue(client.isConnected());
        client.disconnect();
    }

    // 2
    @Test @Order(2)
    void createCsClInsertQuery() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs1");
        client.createCollection("jcs1", "cl1");
        DBCollection cl = client.getCollection("jcs1", "cl1");
        cl.insert(new Document("x", 1).append("name", "alice"));
        cl.insert(new Document("x", 2).append("name", "bob"));
        List<Document> docs = cl.query().collectAll();
        assertEquals(2, docs.size());
    }

    // 3
    @Test @Order(3)
    void insertManyThenQueryCondition() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs2");
        client.createCollection("jcs2", "cl");
        DBCollection cl = client.getCollection("jcs2", "cl");
        List<Document> docs = IntStream.range(0, 5)
                .mapToObj(i -> new Document("val", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        List<Document> results = cl.query(new Document("val", new Document("$gte", 3))).collectAll();
        assertEquals(2, results.size());
    }

    // 4
    @Test @Order(4)
    void updateThenVerify() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs3");
        client.createCollection("jcs3", "cl");
        DBCollection cl = client.getCollection("jcs3", "cl");
        cl.insert(new Document("x", 1).append("y", 10));
        cl.update(new Document("x", 1), new Document("$set", new Document("y", 99)));
        List<Document> docs = cl.query().collectAll();
        assertEquals(1, docs.size());
        assertEquals(99, docs.get(0).getInteger("y"));
    }

    // 5
    @Test @Order(5)
    void deleteThenVerify() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs4");
        client.createCollection("jcs4", "cl");
        DBCollection cl = client.getCollection("jcs4", "cl");
        cl.insertMany(List.of(new Document("x", 1), new Document("x", 2), new Document("x", 3)));
        cl.delete(new Document("x", 2));
        List<Document> docs = cl.query().collectAll();
        assertEquals(2, docs.size());
        List<Integer> vals = docs.stream().map(d -> d.getInteger("x")).collect(Collectors.toList());
        assertTrue(vals.contains(1));
        assertTrue(vals.contains(3));
        assertFalse(vals.contains(2));
    }

    // 6
    @Test @Order(6)
    void createIndexThenQuery() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs5");
        client.createCollection("jcs5", "cl");
        DBCollection cl = client.getCollection("jcs5", "cl");
        List<Document> docs = IntStream.range(0, 10)
                .mapToObj(i -> new Document("val", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        client.createIndex("jcs5", "cl", "idx_val", new Document("val", 1), false);
        List<Document> results = cl.query(new Document("val", 5)).collectAll();
        assertEquals(1, results.size());
        assertEquals(5, results.get(0).getInteger("val"));
    }

    // 7
    @Test @Order(7)
    void dropClDropCs() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcs6");
        client.createCollection("jcs6", "cl");
        DBCollection cl = client.getCollection("jcs6", "cl");
        cl.insert(new Document("a", 1));
        client.dropCollection("jcs6", "cl");
        assertThrows(SdbException.class, () -> cl.query());
        client.dropCollectionSpace("jcs6");
    }

    // 8
    @Test @Order(8)
    void errorNonexistent() {
        SdbClient client = makeClient();
        DBCollection cl = client.getCollection("nope", "nope");
        assertThrows(SdbException.class, () -> cl.query());
        assertThrows(SdbException.class, () -> cl.insert(new Document("x", 1)));
        assertThrows(SdbException.class, () -> client.createCollection("nope", "cl"));
    }

    // 9 (moved before auth tests)
    @Test @Order(9)
    void insertBatch() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jbatchcs");
        client.createCollection("jbatchcs", "cl");
        DBCollection cl = client.getCollection("jbatchcs", "cl");
        List<Document> docs = IntStream.range(0, 50)
                .mapToObj(i -> new Document("i", i))
                .collect(Collectors.toList());
        cl.insertBatch(docs);
        List<Document> results = cl.query().collectAll();
        assertEquals(50, results.size());
    }

    // 10
    @Test @Order(10)
    void authCreateUserAuthenticate() {
        SdbClient client = makeClient();
        client.createUser("jtestuser", "pass123", List.of("admin"));
        client.authenticate("jtestuser", "pass123");
        assertThrows(SdbException.class, () -> client.authenticate("jtestuser", "wrong"));
        client.dropUser("jtestuser");
        assertThrows(SdbException.class, () -> client.authenticate("jtestuser", "pass123"));
    }

    // 11
    @Test @Order(11)
    void aggregateMatchAndLimit() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jaggcs");
        client.createCollection("jaggcs", "cl");
        DBCollection cl = client.getCollection("jaggcs", "cl");
        List<Document> docs = IntStream.range(0, 10)
                .mapToObj(i -> new Document("val", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        List<Document> pipeline = List.of(
                new Document("$match", new Document("val", new Document("$gte", 5))),
                new Document("$limit", 3)
        );
        List<Document> results = cl.aggregate(pipeline);
        assertEquals(3, results.size());
        for (Document r : results) {
            assertTrue(r.getInteger("val") >= 5);
        }
    }

    // 12
    @Test @Order(12)
    void sqlSelect() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jsqlcs");
        client.createCollection("jsqlcs", "cl");
        DBCollection cl = client.getCollection("jsqlcs", "cl");
        cl.insertMany(List.of(
                new Document("name", "alice").append("age", 30),
                new Document("name", "bob").append("age", 25)
        ));
        List<Document> results = client.execSql("SELECT * FROM jsqlcs.cl");
        assertEquals(2, results.size());
    }

    // 13
    @Test @Order(13)
    void serverSideCount() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jcntcs");
        client.createCollection("jcntcs", "cl");
        DBCollection cl = client.getCollection("jcntcs", "cl");
        List<Document> docs = IntStream.range(0, 8)
                .mapToObj(i -> new Document("val", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        assertEquals(8, cl.count());
        assertEquals(3, cl.count(new Document("val", new Document("$gte", 5))));
    }

    // 14
    @Test @Order(14)
    void getMoreCursorCollectAll() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jgmcs");
        client.createCollection("jgmcs", "cl");
        DBCollection cl = client.getCollection("jgmcs", "cl");
        List<Document> docs = IntStream.range(0, 250)
                .mapToObj(i -> new Document("i", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        DBCursor cursor = cl.query();
        List<Document> all = cursor.collectAll();
        assertEquals(250, all.size());
    }

    // 15
    @Test @Order(15)
    void getMoreCursorManualFetch() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jgm2cs");
        client.createCollection("jgm2cs", "cl");
        DBCollection cl = client.getCollection("jgm2cs", "cl");
        List<Document> docs = IntStream.range(0, 150)
                .mapToObj(i -> new Document("i", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        DBCursor cursor = cl.query();

        int count = 0;
        while (cursor.next() != null) count++;
        assertEquals(100, count);
        assertTrue(cursor.hasMore());

        assertTrue(cursor.fetchMore());
        int count2 = 0;
        while (cursor.next() != null) count2++;
        assertEquals(50, count2);
        assertFalse(cursor.hasMore());
    }

    // 16
    @Test @Order(16)
    void cursorCloseKillsServerCursor() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jclcs");
        client.createCollection("jclcs", "cl");
        DBCollection cl = client.getCollection("jclcs", "cl");
        List<Document> docs = IntStream.range(0, 200)
                .mapToObj(i -> new Document("i", i))
                .collect(Collectors.toList());
        cl.insertMany(docs);
        DBCursor cursor = cl.query();
        assertTrue(cursor.hasMore());
        cursor.close();
        assertFalse(cursor.hasMore());
        assertNull(cursor.next());
    }

    // 17
    @Test @Order(17)
    void transactionCommit() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jtxcs");
        client.createCollection("jtxcs", "cl");
        DBCollection cl = client.getCollection("jtxcs", "cl");
        client.transactionBegin();
        cl.insert(new Document("x", 1));
        cl.insert(new Document("x", 2));
        client.transactionCommit();
        List<Document> docs = cl.query().collectAll();
        assertEquals(2, docs.size());
    }

    // 18
    @Test @Order(18)
    void transactionRollback() {
        SdbClient client = makeClient();
        client.createCollectionSpace("jtxcs2");
        client.createCollection("jtxcs2", "cl");
        DBCollection cl = client.getCollection("jtxcs2", "cl");
        cl.insert(new Document("x", 0));
        client.transactionBegin();
        cl.insert(new Document("x", 999));
        client.transactionRollback();
        List<Document> docs = cl.query().collectAll();
        assertEquals(1, docs.size());
        assertEquals(0, docs.get(0).getInteger("x"));
    }

    // 19
    @Test @Order(19)
    void connectionPoolReuse() {
        ConnectOptions opts = new ConnectOptions("127.0.0.1", port);
        opts.setMaxPoolSize(3);
        SdbClient client = new SdbClient(opts);
        client.createCollectionSpace("jpoolcs");
        client.createCollection("jpoolcs", "cl");
        DBCollection cl = client.getCollection("jpoolcs", "cl");
        for (int i = 0; i < 10; i++) {
            cl.insert(new Document("i", i));
        }
        List<Document> docs = cl.query().collectAll();
        assertEquals(10, docs.size());
    }

    // 20
    @Test @Order(20)
    void autoAuthOnConnect() {
        SdbClient client = makeClient();
        client.createUser("jauthuser", "jpass", List.of("admin"));

        ConnectOptions opts = new ConnectOptions("127.0.0.1", port);
        opts.setUsername("jauthuser");
        opts.setPassword("jpass");
        SdbClient client2 = new SdbClient(opts);
        client2.createCollectionSpace("jauthcs");
        client2.createCollection("jauthcs", "cl");
        DBCollection cl = client2.getCollection("jauthcs", "cl");
        cl.insert(new Document("x", 1));
        List<Document> docs = cl.query().collectAll();
        assertEquals(1, docs.size());
    }

}
