from .codec import field, loads, name, scalar_token
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import visible_rows_after


class _UniqueConflict(Exception):
    """Internal signal: the finished build would violate a unique projection."""

    def __init__(self, token, keys):
        super().__init__(token)
        self.token = token
        self.keys = keys


def refresh_document(connection, collection, key, document):
    """Maintain every *ready* index for one committed document.

    Online builds accumulate their partial projection in ``index_build_values``,
    outside ``index_values``, so a live write can never mix post-snapshot values
    into a snapshot scan.  The build replays the commit stream from its snapshot
    revision instead (see ``Indexes.catch_up``).
    """
    connection.execute("DELETE FROM index_values WHERE collection=? AND key=?", (collection, key))
    if document is None:
        return
    for definition in connection.execute("SELECT * FROM index_defs WHERE collection=?", (collection,)).fetchall():
        token = scalar_token(field(document, definition["field"]))
        if token is None:
            continue
        if definition["unique_flag"]:
            existing = connection.execute("SELECT key FROM index_values WHERE collection=? AND name=? AND token=? LIMIT 1",
                                          (collection, definition["name"], token)).fetchone()
            if existing:
                raise Conflict("unique index conflict: " + definition["name"])
        connection.execute("INSERT INTO index_values VALUES (?,?,?,?)", (collection, definition["name"], token, key))


class Indexes:
    """Current index projections plus resumable online build control.

    An online build is a persistent state machine stored in ``index_builds``::

        begin -> scanning --(snapshot exhausted)--> catching_up -> finish -> ready

    ``scanning`` materialises the visible view pinned at ``scan_revision`` in
    bounded key-order chunks.  ``catching_up`` replays the commit stream from
    ``pending_from`` so writes and deletes that happened during scanning (and
    after it) are applied.  ``finish`` replays the remaining commits and switches
    the definition into ``index_defs`` in one short ``BEGIN IMMEDIATE`` critical
    section: that commit is the first revision at which a query may consume the
    index, and the next writer already maintains it.  Every chunk is its own
    transaction, so the build never holds the writer for the whole scan.
    """

    def __init__(self, store):
        self.store = store

    # -- public build control -------------------------------------------------

    def begin(self, collection, index_name, path, unique=False, chunk_size=500):
        """Register (or resume) an online build and return its progress.

        Calling ``begin`` again with the same name and definition resumes the
        persisted build after a close/reopen; a different definition conflicts.
        """
        name(index_name)
        field({}, path)
        if not isinstance(unique, bool):
            raise InvalidDocument("unique must be boolean")
        self._checked_chunk(chunk_size, "build chunk size must be between 1 and 10000")
        with self.store.atomic(writable=True) as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            if connection.execute("SELECT 1 FROM index_defs WHERE collection=? AND name=?", (collection, index_name)).fetchone():
                raise Conflict("index already exists")
            existing = self._build_row(connection, collection, index_name)
            if existing is not None:
                if existing["field"] != path or bool(existing["unique_flag"]) != unique:
                    raise Conflict("an index build with this name has a different definition")
                return self._progress(connection, existing)
            head = int(self.store.get("head"))
            connection.execute("INSERT INTO index_builds VALUES (?,?,?,?,?,?,?,?,?,?)",
                               (collection, index_name, path, int(unique), "scanning", head, None, 0,
                                head + 1, chunk_size))
            return self._progress(connection, self._build_row(connection, collection, index_name))

    def advance(self, collection, index_name, chunk_size=None):
        """Scan one bounded chunk of the snapshot view.

        Entries, the new ``scan_after`` cursor and ``scanned`` commit together,
        so an interrupted process resumes exactly after the last complete chunk.
        Once the snapshot is exhausted the build moves to ``catching_up``.
        """
        if chunk_size is not None:
            self._checked_chunk(chunk_size, "build chunk size must be between 1 and 10000")
        with self.store.atomic(writable=True) as connection:
            build = self._build_row(connection, collection, index_name)
            if build is None:
                raise NotFound("no index build in progress")
            if build["phase"] != "scanning":
                return self._progress(connection, build)
            size = chunk_size if chunk_size is not None else int(build["chunk_size"])
            rows = visible_rows_after(connection, collection, int(build["scan_revision"]), build["scan_after"], size)
            for row in rows:
                token = scalar_token(field(loads(row["body"]), build["field"]))
                if token is not None:
                    connection.execute("INSERT OR REPLACE INTO index_build_values VALUES (?,?,?,?)",
                                       (collection, index_name, token, row["key"]))
            scanned = int(build["scanned"]) + len(rows)
            after = rows[-1]["key"] if rows else build["scan_after"]
            phase = "catching_up" if len(rows) < size else "scanning"
            connection.execute("UPDATE index_builds SET phase=?,scanned=?,scan_after=? WHERE collection=? AND name=?",
                               (phase, scanned, after, collection, index_name))
            return self._progress(connection, self._build_row(connection, collection, index_name))

    def catch_up(self, collection, index_name, limit=200):
        """Replay up to ``limit`` committed revisions into the partial index.

        A delete removes the projection for its key; an insert or update replaces
        it.  ``pending_from`` advances in the same transaction as the replay, so
        repeated calls after a restart never skip a commit.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise InvalidDocument("catch-up limit must be between 1 and 10000")
        with self.store.atomic(writable=True) as connection:
            build = self._build_row(connection, collection, index_name)
            if build is None:
                raise NotFound("no index build in progress")
            if build["phase"] != "catching_up":
                raise Conflict("index scan is not complete; call advance until phase is catching_up")
            head = int(self.store.get("head"))
            pending = int(build["pending_from"])
            if pending <= head:
                commits = connection.execute("SELECT rev,payload FROM commits WHERE rev>=? AND rev<=? ORDER BY rev LIMIT ?",
                                             (pending, head, limit)).fetchall()
                for row in commits:
                    self._apply_commit(connection, build, loads(row["payload"]))
                pending = (commits[-1]["rev"] + 1) if commits else head + 1
                connection.execute("UPDATE index_builds SET pending_from=? WHERE collection=? AND name=?",
                                   (pending, collection, index_name))
            return self._progress(connection, self._build_row(connection, collection, index_name))

    def finish(self, collection, index_name):
        """Atomically catch up the tail and publish the index as ready.

        ``BEGIN IMMEDIATE`` freezes ``head`` for the whole switch: writes that
        committed after the caller's last ``catch_up`` are replayed here, and the
        next writer only starts after ``index_defs`` contains the definition, so
        it maintains the index from its own commit onwards.  There is no revision
        in which the index is visible but stale.
        """
        try:
            with self.store.atomic(writable=True) as connection:
                build = self._build_row(connection, collection, index_name)
                if build is None:
                    definition = connection.execute("SELECT * FROM index_defs WHERE collection=? AND name=?",
                                                    (collection, index_name)).fetchone()
                    if definition is None:
                        raise NotFound("no such index build or index")
                    return {"name": definition["name"], "field": definition["field"],
                            "unique": bool(definition["unique_flag"]), "ready": True,
                            "already_ready": True, "entries": self._entry_count(connection, collection, index_name)}
                if build["phase"] != "catching_up":
                    raise Conflict("index scan is not complete; call advance until phase is catching_up")
                head = int(self.store.get("head"))
                pending = int(build["pending_from"])
                if pending <= head:
                    for row in connection.execute("SELECT rev,payload FROM commits WHERE rev>=? AND rev<=? ORDER BY rev",
                                                  (pending, head)).fetchall():
                        self._apply_commit(connection, build, loads(row["payload"]))
                if build["unique_flag"]:
                    duplicate = connection.execute("""SELECT token, group_concat(key, ',') AS keys FROM index_build_values
                        WHERE collection=? AND name=? GROUP BY token HAVING COUNT(*)>1 ORDER BY token LIMIT 1""",
                                                   (collection, index_name)).fetchone()
                    if duplicate is not None:
                        raise _UniqueConflict(duplicate["token"], duplicate["keys"])
                entries = self._entry_count(connection, collection, index_name)
                connection.execute("INSERT INTO index_defs VALUES (?,?,?,?)",
                                   (collection, index_name, build["field"], build["unique_flag"]))
                connection.execute("""INSERT INTO index_values(collection,name,token,key)
                    SELECT collection,name,token,key FROM index_build_values WHERE collection=? AND name=?""",
                                   (collection, index_name))
                connection.execute("DELETE FROM index_builds WHERE collection=? AND name=?", (collection, index_name))
                self.store.set("catalog_epoch", int(self.store.get("catalog_epoch")) + 1)
                return {"name": index_name, "field": build["field"], "unique": bool(build["unique_flag"]),
                        "ready": True, "already_ready": False, "revision": head,
                        "scan_revision": int(build["scan_revision"]), "entries": entries}
        except _UniqueConflict as conflict:
            try:
                self._discard(collection, index_name)
                cleanup = "build discarded; resolve the duplicates and start a new build"
            except Exception:
                cleanup = "partial build could not be discarded; call abort and start again"
            raise Conflict(f"unique index conflict: {index_name} value {conflict.token} is shared by keys "
                           f"[{conflict.keys}]; {cleanup}") from None

    def abort(self, collection, index_name):
        """Discard a build and its partial projection; a later begin starts fresh."""
        with self.store.atomic(writable=True) as connection:
            if self._build_row(connection, collection, index_name) is None:
                raise NotFound("no index build in progress")
            self._discard_in_transaction(connection, collection, index_name)

    def progress(self, collection, index_name):
        with self.store.read() as connection:
            build = self._build_row(connection, collection, index_name)
            if build is not None:
                return self._progress(connection, build)
            definition = connection.execute("SELECT * FROM index_defs WHERE collection=? AND name=?",
                                            (collection, index_name)).fetchone()
            if definition is None:
                raise NotFound("no such index build or index")
            return {"collection": collection, "name": index_name, "field": definition["field"],
                    "unique": bool(definition["unique_flag"]), "ready": True, "phase": "ready",
                    "entries": self._entry_count(connection, collection, index_name)}

    def create(self, collection, index_name, path, unique=False):
        """Build synchronously through the same chunked state machine.

        The caller blocks, but the database writer does not: each chunk is a
        short transaction and concurrent commits proceed between them.  Any
        failure discards the partial projection, matching the all-or-nothing
        contract of ``create``.
        """
        self.begin(collection, index_name, path, unique)
        try:
            while self.advance(collection, index_name)["phase"] == "scanning":
                pass
            while self.catch_up(collection, index_name)["lag"] > 0:
                pass
            result = self.finish(collection, index_name)
        except BaseException:
            try:
                self.abort(collection, index_name)
            except Exception:
                pass
            raise
        return {"name": result["name"], "field": result["field"], "unique": result["unique"]}

    def drop(self, collection, index_name):
        with self.store.atomic(writable=True) as connection:
            changed = connection.execute("DELETE FROM index_defs WHERE collection=? AND name=?", (collection, index_name)).rowcount
            if not changed:
                raise NotFound("unknown index")
            self.store.set("catalog_epoch", int(self.store.get("catalog_epoch")) + 1)

    def list(self, collection):
        with self.store.read() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM index_defs WHERE collection=? ORDER BY name", (collection,))]

    def builds(self, collection=None):
        """Return progress for every persisted, not-yet-ready build."""
        with self.store.read() as connection:
            if collection is None:
                rows = connection.execute("SELECT * FROM index_builds ORDER BY collection,name").fetchall()
            else:
                rows = connection.execute("SELECT * FROM index_builds WHERE collection=? ORDER BY name", (collection,)).fetchall()
            return [self._progress(connection, row) for row in rows]

    # -- internals ------------------------------------------------------------

    @staticmethod
    def _checked_chunk(chunk_size, message):
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or not 1 <= chunk_size <= 10000:
            raise InvalidDocument(message)

    @staticmethod
    def _build_row(connection, collection, index_name):
        return connection.execute("SELECT * FROM index_builds WHERE collection=? AND name=?",
                                  (collection, index_name)).fetchone()

    @staticmethod
    def _entry_count(connection, collection, index_name):
        ready = connection.execute("SELECT 1 FROM index_defs WHERE collection=? AND name=?",
                                   (collection, index_name)).fetchone()
        table = "index_values" if ready else "index_build_values"
        return connection.execute(f"SELECT COUNT(*) FROM {table} WHERE collection=? AND name=?",
                                  (collection, index_name)).fetchone()[0]

    def _progress(self, connection, build):
        head = int(self.store.get("head"))
        pending = int(build["pending_from"])
        return {"collection": build["collection"], "name": build["name"], "field": build["field"],
                "unique": bool(build["unique_flag"]), "ready": False, "phase": build["phase"],
                "scan_revision": int(build["scan_revision"]), "scanned": int(build["scanned"]),
                "scan_after": build["scan_after"], "pending_from": pending,
                "lag": max(0, head - pending + 1),
                "entries": self._entry_count(connection, build["collection"], build["name"]),
                "chunk_size": int(build["chunk_size"])}

    @staticmethod
    def _apply_commit(connection, build, commit):
        for operation in commit["operations"]:
            if operation["collection"] != build["collection"]:
                continue
            connection.execute("DELETE FROM index_build_values WHERE collection=? AND name=? AND key=?",
                               (build["collection"], build["name"], operation["key"]))
            document = operation["document"]
            if document is None:
                continue
            token = scalar_token(field(document, build["field"]))
            if token is not None:
                connection.execute("INSERT OR REPLACE INTO index_build_values VALUES (?,?,?,?)",
                                   (build["collection"], build["name"], token, operation["key"]))

    @staticmethod
    def _discard_in_transaction(connection, collection, index_name):
        connection.execute("DELETE FROM index_build_values WHERE collection=? AND name=?", (collection, index_name))
        connection.execute("DELETE FROM index_builds WHERE collection=? AND name=?", (collection, index_name))

    def _discard(self, collection, index_name):
        with self.store.atomic(writable=True) as connection:
            self._discard_in_transaction(connection, collection, index_name)


def equality_candidates(connection, collection, path, value):
    """Return candidates from a *ready* index only; builds are never consulted."""
    token = scalar_token(value)
    if token is None:
        return None
    definition = connection.execute("SELECT name FROM index_defs WHERE collection=? AND field=? ORDER BY name LIMIT 1",
                                    (collection, path)).fetchone()
    if definition is None:
        return None
    keys = {row[0] for row in connection.execute("SELECT key FROM index_values WHERE collection=? AND name=? AND token=?",
                                               (collection, definition[0], token))}
    return definition[0], keys
