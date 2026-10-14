"""Execute the lease SQL against real competing SQLite connections.
No claim of Rust compilation/test execution is implied.
"""
import sqlite3
import tempfile
from pathlib import Path
from time import sleep

NOW="CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
DDL="""CREATE TABLE shard_owners (
 shard_id INTEGER PRIMARY KEY,owner_id TEXT NOT NULL,epoch INTEGER NOT NULL,
 endpoint TEXT NOT NULL DEFAULT '',expires_at_ms INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE states(actor_kind TEXT,actor_key TEXT,version INTEGER,payload BLOB,
 PRIMARY KEY(actor_kind,actor_key));"""
CLAIM=f"""INSERT INTO shard_owners(shard_id,owner_id,epoch,endpoint,expires_at_ms)
 VALUES(?1,?2,1,?3,({NOW})+?4)
 ON CONFLICT(shard_id) DO UPDATE SET
 owner_id=excluded.owner_id,epoch=shard_owners.epoch+1,
 endpoint=excluded.endpoint,expires_at_ms=excluded.expires_at_ms
 WHERE shard_owners.expires_at_ms <= ({NOW})"""
RENEW=f"""UPDATE shard_owners SET expires_at_ms=({NOW})+?3
 WHERE shard_id=?1 AND owner_id=?2 AND expires_at_ms > ({NOW})"""
VALID=f"""SELECT owner_id,epoch FROM shard_owners
 WHERE shard_id=?1 AND expires_at_ms > {NOW}"""

def claim(con, shard, owner, ttl):
    con.execute('BEGIN IMMEDIATE')
    try:
        cur=con.execute(CLAIM,(shard,owner,owner+':8000',ttl))
        if cur.rowcount!=1:
            con.rollback()
            return False
        con.commit()
        return True
    except Exception:
        con.rollback();raise

with tempfile.TemporaryDirectory() as td:
    file=Path(td)/'lease.db'
    a=sqlite3.connect(file,isolation_level=None,timeout=1.5)
    b=sqlite3.connect(file,isolation_level=None,timeout=1.5)
    a.executescript(DDL)
    assert claim(a,1,'a',650)
    assert not claim(b,1,'b',650), 'active shard was stolen'
    assert a.execute(VALID,(1,)).fetchone()==('a',1)
    # A commit transaction blocks a concurrent owner transition.
    a.execute('BEGIN IMMEDIATE')
    assert a.execute(VALID,(1,)).fetchone()==('a',1)
    a.execute('INSERT INTO states VALUES (?,?,?,?)',('counter','k',1,b'committed'))
    assert b.execute('SELECT payload FROM states').fetchall()==[], 'uncommitted visible'
    a.commit()
    sleep(.75)
    assert a.execute(VALID,(1,)).fetchone() is None
    assert a.execute(RENEW,(1,'a',1000)).rowcount==0, 'expired owner renewed'
    assert claim(b,1,'b',1000)
    assert b.execute(VALID,(1,)).fetchone()==('b',2)
    assert a.execute(VALID,(1,)).fetchone()!=('a',1), 'old epoch still valid'
    assert b.execute('SELECT payload FROM states').fetchone()==(b'committed',)
    a.close();b.close()
print('PASS: unexpired lease exclusion, expired renewal denial, epoch takeover, WAL isolation')
