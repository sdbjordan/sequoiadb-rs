package com.sequoiadb.driver;

import com.sequoiadb.driver.protocol.*;
import org.bson.Document;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class SdbClient {
    private final ConnectOptions options;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Connection> pool = new ArrayList<>();
    private int poolSize = 0;
    private Connection txnConn = null;
    private boolean connected = false;

    public SdbClient(ConnectOptions options) {
        this.options = options;
        Connection conn = createConnection();
        release(conn);
        this.connected = true;
    }

    public SdbClient(String host, int port) {
        this(new ConnectOptions(host, port));
    }

    private Connection createConnection() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(options.getHost(), options.getPort()),
                    options.getConnectTimeoutMs());
            Connection conn = new Connection(socket);

            if (options.getUsername() != null && options.getPassword() != null) {
                long rid = conn.nextId();
                Document cond = new Document("User", options.getUsername())
                        .append("Passwd", options.getPassword());
                byte[] msg = MsgOpQuery.create(rid, "$authenticate", cond, null, null, null, 0, -1, 0).encode();
                MsgOpReply reply = conn.sendRecv(msg);
                checkReply(reply);
            }

            return conn;
        } catch (IOException e) {
            throw new SdbException(-15, "Connection failed: " + e.getMessage());
        }
    }

    private Connection acquire() {
        lock.lock();
        try {
            while (!pool.isEmpty()) {
                Connection conn = pool.remove(pool.size() - 1);
                if (conn.isAlive()) return conn;
                poolSize--;
            }
        } finally {
            lock.unlock();
        }

        if (poolSize >= options.getMaxPoolSize()) {
            throw new SdbException(-17, "Connection pool exhausted");
        }

        Connection conn = createConnection();
        lock.lock();
        try {
            poolSize++;
        } finally {
            lock.unlock();
        }
        return conn;
    }

    private void release(Connection conn) {
        lock.lock();
        try {
            pool.add(conn);
        } finally {
            lock.unlock();
        }
    }

    MsgOpReply sendRecv(Function<Long, byte[]> buildMsg) {
        if (txnConn != null) {
            long rid = txnConn.nextId();
            return txnConn.sendRecv(buildMsg.apply(rid));
        }
        Connection conn = acquire();
        try {
            long rid = conn.nextId();
            MsgOpReply reply = conn.sendRecv(buildMsg.apply(rid));
            return reply;
        } finally {
            release(conn);
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public void disconnect() {
        try {
            Connection conn = acquire();
            long rid = conn.nextId();
            MsgHeader header = MsgHeader.newRequest(OpCode.DISCONNECT, rid);
            ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE).order(ByteOrder.LITTLE_ENDIAN);
            header.encode(buf);
            buf.putInt(0, buf.position());
            byte[] data = new byte[buf.position()];
            buf.flip();
            buf.get(data);
            try {
                conn.send(data);
            } catch (Exception ignored) {}
        } catch (Exception ignored) {}
        connected = false;
    }

    public DBCollection getCollection(String csName, String clName) {
        return new DBCollection(this, csName + "." + clName);
    }

    public CollectionSpace getCollectionSpace(String name) {
        return new CollectionSpace(this, name);
    }

    // ── DDL ────────────────────────────────────────────────────────

    public void createCollectionSpace(String name) {
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$create collectionspace",
                new Document("Name", name), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
    }

    public void dropCollectionSpace(String name) {
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$drop collectionspace",
                new Document("Name", name), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
    }

    public void createCollection(String cs, String cl) {
        String fullName = cs + "." + cl;
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$create collection",
                new Document("Name", fullName), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
    }

    public void dropCollection(String cs, String cl) {
        String fullName = cs + "." + cl;
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$drop collection",
                new Document("Name", fullName), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
    }

    public void createIndex(String cs, String cl, String idxName, Document key, boolean unique) {
        String fullName = cs + "." + cl;
        MsgOpReply reply = sendRecv(rid -> {
            Document indexDoc = new Document("name", idxName)
                    .append("key", key)
                    .append("unique", unique);
            Document cond = new Document("Collection", fullName)
                    .append("Index", indexDoc);
            return MsgOpQuery.create(rid, "$create index", cond, null, null, null, 0, -1, 0).encode();
        });
        checkReply(reply);
    }

    public void dropIndex(String cs, String cl, String idxName) {
        String fullName = cs + "." + cl;
        MsgOpReply reply = sendRecv(rid -> {
            Document cond = new Document("Collection", fullName)
                    .append("Index", idxName);
            return MsgOpQuery.create(rid, "$drop index", cond, null, null, null, 0, -1, 0).encode();
        });
        checkReply(reply);
    }

    // ── SQL ────────────────────────────────────────────────────────

    public List<Document> execSql(String sql) {
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$sql",
                new Document("SQL", sql), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
        return reply.docs;
    }

    // ── Auth ───────────────────────────────────────────────────────

    public void authenticate(String username, String password) {
        MsgOpReply reply = sendRecv(rid -> {
            Document cond = new Document("User", username).append("Passwd", password);
            return MsgOpQuery.create(rid, "$authenticate", cond, null, null, null, 0, -1, 0).encode();
        });
        checkReply(reply);
    }

    public void createUser(String username, String password, List<String> roles) {
        MsgOpReply reply = sendRecv(rid -> {
            Document cond = new Document("User", username)
                    .append("Passwd", password)
                    .append("Roles", roles);
            return MsgOpQuery.create(rid, "$create user", cond, null, null, null, 0, -1, 0).encode();
        });
        checkReply(reply);
    }

    public void dropUser(String username) {
        MsgOpReply reply = sendRecv(rid ->
            MsgOpQuery.create(rid, "$drop user",
                new Document("User", username), null, null, null, 0, -1, 0).encode()
        );
        checkReply(reply);
    }

    // ── Transactions ──────────────────────────────────────────────

    public void transactionBegin() {
        Connection conn = acquire();
        lock.lock();
        try {
            poolSize--;
        } finally {
            lock.unlock();
        }

        long rid = conn.nextId();
        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        MsgHeader.newRequest(OpCode.TRANS_BEGIN_REQ, rid).encode(buf);
        buf.putInt(0, buf.position());
        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);
        MsgOpReply reply = conn.sendRecv(data);
        checkReply(reply);
        this.txnConn = conn;
    }

    public void transactionCommit() {
        Connection conn = this.txnConn;
        if (conn == null) throw new SdbException(-253, "No active transaction");
        this.txnConn = null;

        long rid = conn.nextId();
        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        MsgHeader.newRequest(OpCode.TRANS_COMMIT_REQ, rid).encode(buf);
        buf.putInt(0, buf.position());
        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);
        MsgOpReply reply = conn.sendRecv(data);
        lock.lock();
        try {
            poolSize++;
        } finally {
            lock.unlock();
        }
        release(conn);
        checkReply(reply);
    }

    public void transactionRollback() {
        Connection conn = this.txnConn;
        if (conn == null) throw new SdbException(-253, "No active transaction");
        this.txnConn = null;

        long rid = conn.nextId();
        ByteBuffer buf = ByteBuffer.allocate(MsgHeader.SIZE).order(ByteOrder.LITTLE_ENDIAN);
        MsgHeader.newRequest(OpCode.TRANS_ROLLBACK_REQ, rid).encode(buf);
        buf.putInt(0, buf.position());
        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);
        MsgOpReply reply = conn.sendRecv(data);
        lock.lock();
        try {
            poolSize++;
        } finally {
            lock.unlock();
        }
        release(conn);
        checkReply(reply);
    }

    // ── Helpers ────────────────────────────────────────────────────

    static void checkReply(MsgOpReply reply) {
        if (reply.flags != 0) {
            throw new SdbException(reply.flags);
        }
    }

    // ── Connection (inner class) ──────────────────────────────────

    static class Connection {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private long nextRequestId = 1;

        Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        long nextId() {
            return nextRequestId++;
        }

        void send(byte[] data) {
            try {
                out.write(data);
                out.flush();
            } catch (IOException e) {
                throw new SdbException(-15, "Send failed: " + e.getMessage());
            }
        }

        MsgOpReply sendRecv(byte[] data) {
            send(data);
            return recvReply();
        }

        private MsgOpReply recvReply() {
            try {
                byte[] lenBuf = readExact(4);
                int msgLen = ByteBuffer.wrap(lenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (msgLen < MsgHeader.SIZE) {
                    throw new SdbException(-6, "Invalid reply length: " + msgLen);
                }
                byte[] fullBuf = new byte[msgLen];
                System.arraycopy(lenBuf, 0, fullBuf, 0, 4);
                byte[] rest = readExact(msgLen - 4);
                System.arraycopy(rest, 0, fullBuf, 4, rest.length);

                MsgHeader header = MsgHeader.decode(fullBuf);
                byte[] payload = new byte[msgLen - MsgHeader.SIZE];
                System.arraycopy(fullBuf, MsgHeader.SIZE, payload, 0, payload.length);
                return MsgOpReply.decode(header, payload);
            } catch (IOException e) {
                throw new SdbException(-16, "Connection closed: " + e.getMessage());
            }
        }

        private byte[] readExact(int n) throws IOException {
            byte[] buf = new byte[n];
            int offset = 0;
            while (offset < n) {
                int read = in.read(buf, offset, n - offset);
                if (read < 0) throw new IOException("EOF");
                offset += read;
            }
            return buf;
        }

        boolean isAlive() {
            return !socket.isClosed() && socket.isConnected();
        }
    }
}
