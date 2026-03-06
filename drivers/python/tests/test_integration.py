"""Integration tests — 20 tests mirroring Rust client tests."""

import pytest
from sequoiadb import SdbClient, SdbError


def make_client(port):
    return SdbClient(host="127.0.0.1", port=port)


# 1
def test_connect_disconnect(server_port):
    client = make_client(server_port)
    assert client.is_connected()
    client.disconnect()


# 2
def test_create_cs_cl_insert_query(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs1")
    client.create_collection("pycs1", "cl1")
    cl = client.get_collection("pycs1", "cl1")
    cl.insert({"x": 1, "name": "alice"})
    cl.insert({"x": 2, "name": "bob"})
    docs = cl.query().collect_all()
    assert len(docs) == 2


# 3
def test_insert_many_then_query_condition(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs2")
    client.create_collection("pycs2", "cl")
    cl = client.get_collection("pycs2", "cl")
    cl.insert_many([{"val": i} for i in range(5)])
    results = cl.query({"val": {"$gte": 3}}).collect_all()
    assert len(results) == 2  # val=3, val=4


# 4
def test_update_then_verify(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs3")
    client.create_collection("pycs3", "cl")
    cl = client.get_collection("pycs3", "cl")
    cl.insert({"x": 1, "y": 10})
    cl.update({"x": 1}, {"$set": {"y": 99}})
    docs = cl.query().collect_all()
    assert len(docs) == 1
    assert docs[0]["y"] == 99


# 5
def test_delete_then_verify(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs4")
    client.create_collection("pycs4", "cl")
    cl = client.get_collection("pycs4", "cl")
    cl.insert_many([{"x": 1}, {"x": 2}, {"x": 3}])
    cl.delete({"x": 2})
    docs = cl.query().collect_all()
    assert len(docs) == 2
    vals = [d["x"] for d in docs]
    assert 1 in vals
    assert 3 in vals
    assert 2 not in vals


# 6
def test_create_index_then_query(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs5")
    client.create_collection("pycs5", "cl")
    cl = client.get_collection("pycs5", "cl")
    cl.insert_many([{"val": i} for i in range(10)])
    client.create_index("pycs5", "cl", "idx_val", {"val": 1}, unique=False)
    results = cl.query({"val": 5}).collect_all()
    assert len(results) == 1
    assert results[0]["val"] == 5


# 7
def test_drop_cl_drop_cs(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycs6")
    client.create_collection("pycs6", "cl")
    cl = client.get_collection("pycs6", "cl")
    cl.insert({"a": 1})
    client.drop_collection("pycs6", "cl")
    with pytest.raises(SdbError):
        cl.query()
    client.drop_collection_space("pycs6")


# 8
def test_error_nonexistent(server_port):
    client = make_client(server_port)
    cl = client.get_collection("nope", "nope")
    with pytest.raises(SdbError):
        cl.query()
    with pytest.raises(SdbError):
        cl.insert({"x": 1})
    with pytest.raises(SdbError):
        client.create_collection("nope", "cl")


# 9 (moved before auth tests to avoid auth-enforcement issues)
def test_insert_batch(server_port):
    client = make_client(server_port)
    client.create_collection_space("pybatchcs")
    client.create_collection("pybatchcs", "cl")
    cl = client.get_collection("pybatchcs", "cl")
    cl.insert_batch([{"i": i} for i in range(50)])
    docs = cl.query().collect_all()
    assert len(docs) == 50


# 10
def test_auth_create_user_authenticate(server_port):
    client = make_client(server_port)
    client.create_user("pytestuser", "pass123", ["admin"])
    client.authenticate("pytestuser", "pass123")
    with pytest.raises(SdbError):
        client.authenticate("pytestuser", "wrong")
    client.drop_user("pytestuser")
    with pytest.raises(SdbError):
        client.authenticate("pytestuser", "pass123")


# 10
def test_aggregate_match_and_limit(server_port):
    client = make_client(server_port)
    client.create_collection_space("pyaggcs")
    client.create_collection("pyaggcs", "cl")
    cl = client.get_collection("pyaggcs", "cl")
    cl.insert_many([{"val": i} for i in range(10)])
    pipeline = [
        {"$match": {"val": {"$gte": 5}}},
        {"$limit": 3},
    ]
    results = cl.aggregate(pipeline)
    assert len(results) == 3
    for r in results:
        assert r["val"] >= 5


# 11
def test_sql_select(server_port):
    client = make_client(server_port)
    client.create_collection_space("pysqlcs")
    client.create_collection("pysqlcs", "cl")
    cl = client.get_collection("pysqlcs", "cl")
    cl.insert_many([
        {"name": "alice", "age": 30},
        {"name": "bob", "age": 25},
    ])
    results = client.exec_sql("SELECT * FROM pysqlcs.cl")
    assert len(results) == 2


# 12
def test_server_side_count(server_port):
    client = make_client(server_port)
    client.create_collection_space("pycntcs")
    client.create_collection("pycntcs", "cl")
    cl = client.get_collection("pycntcs", "cl")
    cl.insert_many([{"val": i} for i in range(8)])
    assert cl.count() == 8
    assert cl.count({"val": {"$gte": 5}}) == 3  # val=5,6,7


# 13
def test_get_more_cursor_collect_all(server_port):
    client = make_client(server_port)
    client.create_collection_space("pygmcs")
    client.create_collection("pygmcs", "cl")
    cl = client.get_collection("pygmcs", "cl")
    cl.insert_many([{"i": i} for i in range(250)])
    cursor = cl.query()
    all_docs = cursor.collect_all()
    assert len(all_docs) == 250


# 14
def test_get_more_cursor_manual_fetch(server_port):
    client = make_client(server_port)
    client.create_collection_space("pygm2cs")
    client.create_collection("pygm2cs", "cl")
    cl = client.get_collection("pygm2cs", "cl")
    cl.insert_many([{"i": i} for i in range(150)])
    cursor = cl.query()

    count = 0
    while True:
        doc = cursor.next()
        if doc is None:
            break
        count += 1
    assert count == 100
    assert cursor.has_more()

    got_more = cursor.fetch_more()
    assert got_more

    count2 = 0
    while True:
        doc = cursor.next()
        if doc is None:
            break
        count2 += 1
    assert count2 == 50
    assert not cursor.has_more()


# 15
def test_cursor_close_kills_server_cursor(server_port):
    client = make_client(server_port)
    client.create_collection_space("pyclcs")
    client.create_collection("pyclcs", "cl")
    cl = client.get_collection("pyclcs", "cl")
    cl.insert_many([{"i": i} for i in range(200)])
    cursor = cl.query()
    assert cursor.has_more()
    cursor.close()
    assert not cursor.has_more()
    assert cursor.next() is None


# 16
def test_transaction_commit(server_port):
    client = make_client(server_port)
    client.create_collection_space("pytxcs")
    client.create_collection("pytxcs", "cl")
    cl = client.get_collection("pytxcs", "cl")
    client.transaction_begin()
    cl.insert({"x": 1})
    cl.insert({"x": 2})
    client.transaction_commit()
    docs = cl.query().collect_all()
    assert len(docs) == 2


# 17
def test_transaction_rollback(server_port):
    client = make_client(server_port)
    client.create_collection_space("pytxcs2")
    client.create_collection("pytxcs2", "cl")
    cl = client.get_collection("pytxcs2", "cl")
    cl.insert({"x": 0})
    client.transaction_begin()
    cl.insert({"x": 999})
    client.transaction_rollback()
    docs = cl.query().collect_all()
    assert len(docs) == 1
    assert docs[0]["x"] == 0


# 18
def test_connection_pool_reuse(server_port):
    client = SdbClient(host="127.0.0.1", port=server_port, max_pool_size=3)
    client.create_collection_space("pypoolcs")
    client.create_collection("pypoolcs", "cl")
    cl = client.get_collection("pypoolcs", "cl")
    for i in range(10):
        cl.insert({"i": i})
    docs = cl.query().collect_all()
    assert len(docs) == 10


# 19
def test_auto_auth_on_connect(server_port):
    client = make_client(server_port)
    client.create_user("pyauthuser", "pypass", ["admin"])

    client2 = SdbClient(
        host="127.0.0.1",
        port=server_port,
        username="pyauthuser",
        password="pypass",
    )
    client2.create_collection_space("pyauthcs")
    client2.create_collection("pyauthcs", "cl")
    cl = client2.get_collection("pyauthcs", "cl")
    cl.insert({"x": 1})
    docs = cl.query().collect_all()
    assert len(docs) == 1


