import json, sqlite3, threading
from contextlib import contextmanager
from dataclasses import asdict
from fastapi import HTTPException
from src.config.settings import DATABASE_URL
TABLES = ("datasets", "state_models", "studies", "runs", "reviews", "reports", "users", "audit")
_LOCK = threading.RLock()
def _is_sqlite():
    return DATABASE_URL.startswith("sqlite:///")
def connect():
    if _is_sqlite():
        conn = sqlite3.connect(DATABASE_URL.removeprefix("sqlite:///"), timeout=30, isolation_level=None)
        conn.execute("PRAGMA busy_timeout=30000")
        return conn, "?"
    import psycopg
    return psycopg.connect(DATABASE_URL), "%s"
def _begin(conn):
    # SQLite takes the database write lock immediately. PostgreSQL starts a
    # transaction implicitly on the first statement, so only row-level
    # SELECT ... FOR UPDATE fencing is needed there.
    if _is_sqlite():
        conn.execute("BEGIN IMMEDIATE")
def init_db():
    conn,_ = connect()
    try:
        if not _is_sqlite():
            conn.execute("SELECT pg_advisory_xact_lock(2409)")
        for table in TABLES:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.commit()
    finally: conn.close()
def all_rows(table):
    assert table in TABLES
    conn,_ = connect()
    try: return [json.loads(row[0]) for row in conn.execute(f"SELECT payload FROM {table} ORDER BY id").fetchall()]
    finally: conn.close()
def get(table, id):
    assert table in TABLES
    conn,q = connect()
    try:
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}", (id,)).fetchone()
        if row is None: raise HTTPException(404, "记录不存在")
        return json.loads(row[0])
    finally: conn.close()
def read(conn, table, id, q, lock=False, missing_ok=False):
    """Read one payload inside an open transaction, optionally taking a row lock."""
    assert table in TABLES
    suffix = " FOR UPDATE" if lock and not _is_sqlite() else ""
    row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}{suffix}", (id,)).fetchone()
    if row is None:
        if missing_ok: return None
        raise HTTPException(404, "记录不存在")
    return json.loads(row[0])
def rows_in(conn, table, q):
    assert table in TABLES
    return [json.loads(row[0]) for row in conn.execute(f"SELECT payload FROM {table} ORDER BY id").fetchall()]
def upsert(conn, table, item, q):
    assert table in TABLES
    if not isinstance(item, dict): item = asdict(item)
    conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
    return item
@contextmanager
def transaction():
    conn,q = connect()
    try:
        _begin(conn)
        yield conn,q
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally: conn.close()
def put(table, item, insert_only=False):
    assert table in TABLES
    if not isinstance(item, dict): item = asdict(item)
    conn,q = connect()
    clause = "DO NOTHING" if insert_only else "DO UPDATE SET payload=excluded.payload"
    try:
        with _LOCK:
            conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) {clause}", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
            conn.commit()
        return item
    finally: conn.close()
