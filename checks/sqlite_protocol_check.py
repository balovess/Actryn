"""Dependency-free SQLite protocol smoke checks. Run: python checks/sqlite_protocol_check.py

These verify the SQL schema and transactional/fencing ideas; NOT a substitute
for compiling the Rust code and running cargo test.
"""
import pathlib, re, sqlite3, tempfile, threading, time
source = pathlib.Path(__file__).resolve().parents[1] / 'src' / 'storage' / 'sqlite.rs'
text = source.read_text()
sql = re.search(
    r'conn\.execute_batch\(\s*"PRAGMA journal_mode=WAL;(.*?)"\s*,?\s*\)\s*\.map_err\(db_err\)\?',
    text,
    re.S,
)
assert sql, 'could not extract DDL from Rust storage implementation'
schema = 'PRAGMA journal_mode=WAL;' + sql.group(1)

with tempfile.TemporaryDirectory() as d:
    db = pathlib.Path(d)/'check.sqlite'
    con = sqlite3.connect(db,timeout=5, isolation_level=None,check_same_thread=False)
    con.executescript(schema)
    for table in ('states','receipts','shard_owners','outbox'):
        assert con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()

    def claim(c, owner):
        c.execute('BEGIN IMMEDIATE')
        c.execute('INSERT INTO shard_owners(shard_id,owner_id,epoch) VALUES(?,?,1) ON CONFLICT(shard_id) DO UPDATE SET owner_id=excluded.owner_id,epoch=shard_owners.epoch+1',(1,owner))
        ep=c.execute('SELECT epoch FROM shard_owners WHERE shard_id=1').fetchone()[0]
        c.commit()
        return ep
    old_epoch = claim(con,'old')
    assert old_epoch==1
    # A stale owner starts transaction before handover; handover must wait.
    old_commit_started=threading.Event()
    release_commit=threading.Event()
    took_over=threading.Event()
    def old_writer():
        c=sqlite3.connect(db,timeout=5,isolation_level=None)
        c.execute('BEGIN IMMEDIATE')
        assert c.execute('SELECT owner_id,epoch FROM shard_owners WHERE shard_id=1').fetchone()==('old',old_epoch)
        old_commit_started.set()
        release_commit.wait(timeout=3)
        c.execute("INSERT INTO states(actor_kind,actor_key,version,payload) VALUES('a','x',1,?)",(b'one',))
        c.execute("INSERT INTO receipts(actor_kind,actor_key,request_id,request_hash,reply) VALUES('a','x','req',?,?)",(b'0'*32,b'ok'))
        c.execute("INSERT INTO outbox(actor_kind,actor_key,request_id,ordinal,destination_kind,destination_key,payload) VALUES('a','x','req',0,'other','key',?)",(b'evt',))
        c.commit()
        c.close()
    def new_owner():
        c=sqlite3.connect(db,timeout=5,isolation_level=None)
        claim(c,'new')
        took_over.set()
        c.close()
    t1=threading.Thread(target=old_writer);t1.start()
    assert old_commit_started.wait(3)
    t2=threading.Thread(target=new_owner);t2.start()
    time.sleep(.1)
    assert not took_over.is_set(),'takeover interleaved with pending writer transaction'
    release_commit.set()
    t1.join(3);t2.join(3)
    assert took_over.is_set()
    assert con.execute('SELECT owner_id,epoch FROM shard_owners WHERE shard_id=1').fetchone()==('new',2)
    assert con.execute('SELECT payload FROM states').fetchone()==(b'one',)
    assert con.execute('SELECT count(*) FROM outbox').fetchone()[0]==1

    # New transaction by old epoch may be attempted but MUST fail fence check.
    con.execute('BEGIN IMMEDIATE')
    assert con.execute('SELECT owner_id,epoch FROM shard_owners WHERE shard_id=1').fetchone()!=('old',old_epoch)
    con.rollback()

    # Rollback after partial writes never leaks speculative state/outbox.
    con.execute('BEGIN IMMEDIATE')
    con.execute("UPDATE states SET version=2,payload='speculative' WHERE actor_kind='a' AND actor_key='x'")
    con.execute("INSERT INTO outbox(actor_kind,actor_key,request_id,ordinal,destination_kind,destination_key,payload) VALUES('a','x','failed',0,'other','key','not_committed')")
    con.rollback()
    assert con.execute('SELECT version FROM states').fetchone()[0]==1
    assert con.execute('SELECT count(*) FROM outbox').fetchone()[0]==1
    con.close()
print('SQLite schema / ownership serialization / atomic rollback: PASS')
