import json
import sys
import tempfile
from pathlib import Path
from strataleaf import Database, Archives, Conflict, InvalidDocument, InvalidCursor, ReadOnly, SnapshotTooOld, ReplicationError

SCHEMA = {"fields": {"region": "string", "sequence": "integer", "code": "string"}, "required": ["region", "sequence"]}


def seed(db):
    db.catalog.create("segments", SCHEMA)
    batch = db.batch("initial_segments")
    for index in range(6):
        batch.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
    return batch.commit().revision


def rejected(call, error):
    try:
        call()
    except error as exc:
        return {"error": type(exc).__name__, "message": str(exc)}
    raise RuntimeError("operation unexpectedly succeeded")


def run(flow):
    with tempfile.TemporaryDirectory(prefix="strataleaf-example-") as directory:
        folder = Path(directory)
        with Database(folder / "source.db") as db:
            first = seed(db)
            if flow == "catalog":
                output = {"collections": db.catalog.list(), "duplicate": rejected(lambda: db.catalog.create("segments"), Conflict)}
            elif flow == "schema":
                updated = {**SCHEMA, "fields": {**SCHEMA["fields"], "note": "string"}}
                generation = db.catalog.evolve("segments", updated, 1)
                failure = rejected(lambda: db.catalog.evolve("segments", {**updated, "required": ["note"]}, generation), InvalidDocument)
                output = {"generation": generation, "after_rejection": db.catalog.describe("segments"), "incompatible": failure}
            elif flow == "commit":
                db.catalog.create("marks")
                result = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                replay = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                conflict = rejected(lambda: db.batch().put("marks", "discard", {}).put("segments", "seg_01", {"region": "x", "sequence": 1}, expected=0).commit(), Conflict)
                if db.get("marks", "discard") is not None or not replay.replayed:
                    raise RuntimeError("atomicity or replay observation failed")
                output = {"commit": result.__dict__, "replay": replay.__dict__, "conflict": conflict}
            elif flow == "snapshot":
                db.batch().delete("segments", "seg_00", expected=first).commit()
                output = {"past": db.get("segments", "seg_00", first).__dict__, "present": db.get("segments", "seg_00"),
                          "future": rejected(lambda: db.get("segments", "seg_00", db.revision + 1), InvalidDocument)}
            elif flow == "query":
                query = {"field": "sequence", "op": "ge", "value": 0}
                page = db.query("segments", query, limit=2)
                db.batch().delete("segments", "seg_02").put("segments", "seg_07", {"region": "west", "sequence": 7}).commit()
                keys = [row.key for row in page.records]
                cursor = page.cursor
                while cursor:
                    next_page = db.query("segments", query, limit=2, cursor=cursor)
                    keys.extend(row.key for row in next_page.records)
                    cursor = next_page.cursor
                if keys != [f"seg_{i:02}" for i in range(6)]:
                    raise RuntimeError("read revision changed during pagination")
                output = {"revision": page.revision, "keys": keys, "foreign_query": rejected(lambda: db.query("segments", cursor=page.cursor), InvalidCursor)}
            elif flow == "index":
                db.indexes.create("segments", "by_region", "region")
                predicate = {"field": "region", "op": "eq", "value": "east"}
                indexed = db.query("segments", predicate)
                scanned = db.query("segments", predicate, force_scan=True)
                if indexed.records != scanned.records:
                    raise RuntimeError("index and scan differ")

                # Online build: scan in bounded chunks, let writes/deletes land
                # in between, resume through a reopen, then switch atomically.
                steps = [db.indexes.begin("segments", "by_code", "code", unique=True, chunk_size=2)]
                steps.append(db.indexes.advance("segments", "by_code"))
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 0, "code": "c0"}).commit()
                db.batch().delete("segments", "seg_03").commit()
                db.batch().put("segments", "seg_06", {"region": "west", "sequence": 6, "code": "c6"}).commit()
                during_build = db.query("segments", {"field": "code", "op": "eq", "value": "c0"})
                blocked_compaction = db.retention.compact(db.revision)
                while True:
                    state = db.indexes.advance("segments", "by_code")
                    steps.append(state)
                    if state["phase"] != "scanning":
                        break
                db.close()
                with Database(folder / "source.db") as reopened:
                    resumed = reopened.indexes.progress("segments", "by_code")
                    catchup = [reopened.indexes.catch_up("segments", "by_code", limit=1)]
                    catchup.append(reopened.indexes.catch_up("segments", "by_code"))
                    reopened.batch().put("segments", "seg_06", {"region": "west", "sequence": 6, "code": "c16"}).commit()
                    ready = reopened.indexes.finish("segments", "by_code")
                    built = reopened.query("segments", {"field": "code", "op": "eq", "value": "c16"})
                    built_scan = reopened.query("segments", {"field": "code", "op": "eq", "value": "c16"}, force_scan=True)
                    if built.plan["access"] != "equality_index" or built.records != built_scan.records:
                        raise RuntimeError("published index was not used or disagreed with the scan")

                    # A concurrent duplicate detected at switch time fails clearly,
                    # discards the partial projection, and allows a fresh build.
                    reopened.indexes.begin("segments", "unique_sequence", "sequence", unique=True, chunk_size=2)
                    while reopened.indexes.advance("segments", "unique_sequence")["phase"] == "scanning":
                        pass
                    reopened.batch().put("segments", "twin_a", {"region": "east", "sequence": 42, "code": "c42"}).put(
                        "segments", "twin_b", {"region": "west", "sequence": 42, "code": "c43"}).commit()
                    unique_conflict = rejected(lambda: reopened.indexes.finish("segments", "unique_sequence"), Conflict)
                    after_failure = {"builds": reopened.indexes.builds("segments"),
                                     "definitions": [item["name"] for item in reopened.indexes.list("segments")]}
                    reopened.batch().delete("segments", "twin_b").commit()
                    reopened.indexes.begin("segments", "unique_sequence", "sequence", unique=True, chunk_size=2)
                    while reopened.indexes.advance("segments", "unique_sequence")["phase"] == "scanning":
                        pass
                    while reopened.indexes.catch_up("segments", "unique_sequence")["lag"] > 0:
                        pass
                    retried = reopened.indexes.finish("segments", "unique_sequence")

                    collision = rejected(lambda: reopened.batch().put("segments", "new", {"region": "north", "sequence": 8, "code": "c0"}).commit(), Conflict)
                    output = {"plan": indexed.plan, "keys": [row.key for row in indexed.records],
                              "online": {"begin": steps[0], "steps": steps, "query_during_build": during_build.plan,
                                         "blocked_compaction": blocked_compaction, "resumed": resumed,
                                         "catchup": catchup, "ready": ready, "after_switch": built.plan},
                              "unique_conflict": unique_conflict, "after_failure": after_failure, "retried": retried,
                              "collision": collision, "integrity": reopened.diagnostics.inspect(True)}
            elif flow == "feed":
                db.feed.register("mirror", 0)
                db.batch().delete("segments", "seg_00").commit()
                commits = db.feed.read(0, limit=1)
                db.feed.acknowledge("mirror", commits[-1]["revision"])
                output = {"complete_commit": commits, "ack": db.feed.position("mirror"),
                          "backward": rejected(lambda: db.feed.acknowledge("mirror", 0), Conflict)}
            elif flow == "backup":
                backup = db.archives.backup(folder / "backup.db")
                db.batch().delete("segments", "seg_00").commit()
                bad = rejected(lambda: Archives.restore(folder / "backup.db", folder / "invalid.db", "wrong"), Exception)
                Archives.restore(folder / "backup.db", folder / "restored.db", backup["sha256"])
                with Database(folder / "restored.db") as restored:
                    output = {"backup": backup, "restored": restored.diagnostics.fingerprint(), "bad_checksum": bad}
                    if restored.revision != first:
                        raise RuntimeError("backup read boundary changed")
            elif flow == "replica":
                db.archives.backup(folder / "base.db")
                Archives.restore(folder / "base.db", folder / "replica.db", follower=True)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 19}).commit()
                db.batch().delete("segments", "seg_01").commit()
                db.indexes.create("segments", "by_region", "region")
                with Database(folder / "replica.db") as replica:
                    part = db.replication.export(replica.revision, limit=1)
                    applied = replica.replication.apply(part)
                    tail = db.replication.export(replica.revision)
                    replica.replication.apply(tail)
                    repeat = replica.replication.apply(tail)
                    readonly = rejected(lambda: replica.batch().delete("segments", "seg_00").commit(), ReadOnly)
                    if replica.diagnostics.fingerprint() != db.diagnostics.fingerprint() or not replica.diagnostics.inspect(True)["ok"]:
                        raise RuntimeError("replica differs from source")
                    output = {"first_packet": applied, "repeat": repeat, "readonly": readonly, "fingerprint": replica.diagnostics.fingerprint()}
            elif flow == "compact":
                db.retention.pin("reader", first)
                db.feed.register("slow_copy", first)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 8}).commit()
                blocked = db.retention.compact(db.revision)
                old = db.get("segments", "seg_00", first)
                db.retention.unpin("reader")
                db.feed.acknowledge("slow_copy", db.revision)
                advanced = db.retention.compact(db.revision)
                expired = rejected(lambda: db.get("segments", "seg_00", first), SnapshotTooOld)
                replay = db.batch("initial_segments")
                for index in range(6):
                    replay.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
                output = {"blocked": blocked, "old_value": old.document, "advanced": advanced, "expired_read": expired, "replay_after_compaction": replay.commit().__dict__}
            elif flow == "expire":
                written = db.batch().put("segments", "timed", {"region": "north", "sequence": 10}, expires_at=100).commit()
                before = db.get("segments", "timed")
                swept = db.retention.sweep(100)
                if db.get("segments", "timed") is not None or db.get("segments", "timed", written.revision) is None:
                    raise RuntimeError("expiry did not preserve historical visibility")
                output = {"before": before.__dict__, "sweep": swept, "feed": db.feed.read(written.revision), "idle": db.retention.sweep(100)}
            elif flow == "aggregate":
                output = db.aggregates.group("segments", "region", "sequence", revision=first)
                if sum(item["count"] for item in output["groups"]) != 6:
                    raise RuntimeError("aggregation lost visible documents")
                output["invalid"] = rejected(lambda: db.aggregates.group("unknown", "region"), Exception)
            elif flow == "patch":
                result = db.patch("segments", "seg_00", {"code": None, "detail": {"block": 4}}, expected=first)
                output = {"patch": result.__dict__, "document": db.get("segments", "seg_00").document,
                          "stale": rejected(lambda: db.patch("segments", "seg_00", {"sequence": 8}, expected=first), Conflict)}
            elif flow == "inspect":
                db.indexes.create("segments", "by_code", "code")
                output = {"inspection": db.diagnostics.inspect(deep=True), "fingerprint": db.diagnostics.fingerprint(), "checkpoint": db.checkpoint()}
                if not output["inspection"]["ok"]:
                    raise RuntimeError("integrity inspection failed")
            elif flow == "transfer":
                exported = db.transfer.export_lines("segments")
                db.catalog.create("copy_segments", SCHEMA)
                imported = db.transfer.import_lines("copy_segments", exported["lines"])
                output = {"revision": exported["revision"], "imported": imported.__dict__,
                          "duplicate": rejected(lambda: db.transfer.import_lines("copy_segments", exported["lines"]), Conflict)}
                if len(db.query("copy_segments").records) != 6:
                    raise RuntimeError("logical import differs")
            else:
                raise ValueError("unknown example workflow: " + flow)
            print(json.dumps({"workflow": flow, "result": output}, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "commit")
