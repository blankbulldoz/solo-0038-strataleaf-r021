from dataclasses import dataclass
from .codec import loads
from .errors import InvalidDocument, NotFound, SnapshotTooOld


@dataclass(frozen=True)
class Record:
    collection: str
    key: str
    revision: int
    document: dict
    expires_at: float | None = None


def checked_revision(store, revision=None):
    head, floor = int(store.get("head")), int(store.get("floor"))
    revision = head if revision is None else revision
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0 or revision > head:
        raise InvalidDocument("read revision must be between zero and current head")
    if revision < floor:
        raise SnapshotTooOld(f"revision {revision} is older than retained floor {floor}")
    return revision


def latest_row(connection, collection, key, revision):
    return connection.execute("SELECT * FROM versions WHERE collection=? AND key=? AND rev<=? ORDER BY rev DESC LIMIT 1",
                              (collection, key, revision)).fetchone()


def visible_rows(connection, collection, revision):
    return connection.execute("""SELECT v.* FROM versions v JOIN
      (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? GROUP BY key) latest
      ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
      WHERE v.body IS NOT NULL ORDER BY v.key""", (collection, revision, collection)).fetchall()


def visible_rows_after(connection, collection, revision, after, limit):
    """Return one key-ordered slice of the visible view at ``revision``."""
    if after is None:
        sql = """SELECT v.* FROM versions v JOIN
          (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? GROUP BY key) latest
          ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
          WHERE v.body IS NOT NULL ORDER BY v.key LIMIT ?"""
        parameters = (collection, revision, collection, limit)
    else:
        sql = """SELECT v.* FROM versions v JOIN
          (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? AND key>? GROUP BY key) latest
          ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
          WHERE v.body IS NOT NULL ORDER BY v.key LIMIT ?"""
        parameters = (collection, revision, after, collection, limit)
    return connection.execute(sql, parameters).fetchall()


def visible_key_count(connection, collection, revision):
    return connection.execute("""SELECT COUNT(*) FROM versions v JOIN
      (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? GROUP BY key) latest
      ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
      WHERE v.body IS NOT NULL""", (collection, revision, collection)).fetchone()[0]


def record(row):
    if row is None or row["body"] is None:
        return None
    return Record(row["collection"], row["key"], row["rev"], loads(row["body"]), row["expires"])


def read_record(store, collection, key, revision=None):
    with store.read() as connection:
        if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
            raise NotFound("unknown collection: " + collection)
        revision = checked_revision(store, revision)
        return record(latest_row(connection, collection, key, revision))
