#!/usr/bin/env python3
"""入库端（端口默认 8100）。

职责：
  - 接收采集进程上报的发现集合（/expected）与数据批次（/batch）
  - 按 (task_id, epoch) 围栏校验：epoch 落后于调度器推送的当前值 → 409，
    旧进程无法提交旧进度
  - 幂等去重：(task_id, item_id, part) 主键，重复上报记 duplicates
  - 完整性：重算校验和；按任务统计 应收/已收/缺失/重复/被拒
  - 任务定义由调度器同步推送（POST /task），因此调度器短暂离线期间
    入库端仍能按最后一次已知的 epoch 独立校验
"""
import argparse
import json
import threading

from common import (PARTS, checksum_of, db, now, read_json, row, rows,
                    send_json, serve)

LOCK = threading.Lock()
CONN = None


def init_db(path):
    global CONN
    CONN = db(path)
    CONN.execute("""CREATE TABLE IF NOT EXISTS tasks(
        task_id INTEGER PRIMARY KEY, spec TEXT, epoch INTEGER,
        discovery_done INTEGER DEFAULT 0, updated_ts REAL)""")
    CONN.execute("""CREATE TABLE IF NOT EXISTS expected(
        task_id INTEGER, item_id INTEGER, PRIMARY KEY(task_id, item_id))""")
    CONN.execute("""CREATE TABLE IF NOT EXISTS items(
        task_id INTEGER, item_id INTEGER, part TEXT, payload TEXT, checksum TEXT,
        collector_id TEXT, epoch INTEGER, received_ts REAL,
        PRIMARY KEY(task_id, item_id, part))""")
    CONN.execute("""CREATE TABLE IF NOT EXISTS submissions(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, task_id INTEGER, epoch INTEGER,
        collector_id TEXT, kind TEXT, n_records INTEGER, n_new INTEGER, n_dup INTEGER,
        n_bad INTEGER, result TEXT, message TEXT)""")
    CONN.commit()


def get_task(task_id):
    return row(CONN, "SELECT * FROM tasks WHERE task_id=?", (task_id,))


def record_submission(task_id, epoch, collector, kind, n, n_new, n_dup, n_bad,
                      result, message):
    CONN.execute(
        "INSERT INTO submissions(ts,task_id,epoch,collector_id,kind,n_records,n_new,"
        "n_dup,n_bad,result,message) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (now(), task_id, epoch, collector, kind, n, n_new, n_dup, n_bad, result, message))
    CONN.commit()


# ---------------------------------------------------------------- 路由处理

def h_task_upsert(h, p, q):
    """调度器推送任务定义/epoch（单调递增；转移任务时先推新 epoch 再生效）。"""
    body = read_json(h)
    tid = int(body["task_id"])
    epoch = int(body["epoch"])
    spec = json.dumps(body["spec"])
    with LOCK:
        cur = get_task(tid)
        if cur and epoch < cur["epoch"]:
            send_json(h, 409, {"error": "epoch regression", "current_epoch": cur["epoch"]})
            return
        CONN.execute(
            "INSERT INTO tasks(task_id,spec,epoch,updated_ts) VALUES(?,?,?,?) "
            "ON CONFLICT(task_id) DO UPDATE SET spec=excluded.spec,"
            " epoch=excluded.epoch, updated_ts=excluded.updated_ts",
            (tid, spec, epoch, now()))
        CONN.commit()
    send_json(h, 200, {"ok": True, "task_id": tid, "epoch": epoch})


def _check_epoch(h, tid, epoch):
    t = get_task(tid)
    if not t:
        send_json(h, 404, {"error": "unknown task", "task_id": tid})
        return None
    if epoch != t["epoch"]:
        send_json(h, 409, {"error": "stale epoch", "current_epoch": t["epoch"]})
        return None
    return t


def h_expected(h, p, q):
    """采集进程上报 list 接口发现的 item 集合。final=true 表示发现阶段结束。"""
    body = read_json(h)
    tid, epoch = int(body["task_id"]), int(body["epoch"])
    ids = [int(i) for i in body.get("item_ids", [])]
    with LOCK:
        t = _check_epoch(h, tid, epoch)
        if not t:
            record_submission(tid, epoch, body.get("collector_id", ""), "expected",
                              len(ids), 0, 0, 0, "fenced", "stale epoch")
            return
        before = CONN.execute("SELECT COUNT(*) FROM expected WHERE task_id=?",
                              (tid,)).fetchone()[0]
        CONN.executemany("INSERT OR IGNORE INTO expected(task_id,item_id) VALUES(?,?)",
                         [(tid, i) for i in ids])
        if body.get("final"):
            CONN.execute("UPDATE tasks SET discovery_done=1 WHERE task_id=?", (tid,))
        CONN.commit()
        after = CONN.execute("SELECT COUNT(*) FROM expected WHERE task_id=?",
                             (tid,)).fetchone()[0]
        record_submission(tid, epoch, body.get("collector_id", ""), "expected",
                          len(ids), after - before, len(ids) - (after - before), 0,
                          "ok", "")
    send_json(h, 200, {"ok": True, "expected_total": after})


def h_batch(h, p, q):
    body = read_json(h)
    tid, epoch = int(body["task_id"]), int(body["epoch"])
    collector = body.get("collector_id", "")
    records = body.get("records", [])
    with LOCK:
        t = _check_epoch(h, tid, epoch)
        if not t:
            record_submission(tid, epoch, collector, "batch", len(records),
                              0, 0, 0, "fenced", "stale epoch")
            return
        spec = json.loads(t["spec"])
        parts_ok = set(spec.get("parts", ["detail"]))
        n_new = n_dup = n_bad = 0
        for r in records:
            item_id, part = int(r.get("item_id", 0)), r.get("part", "")
            payload = r.get("payload", {})
            content = payload.get("body") if part == "detail" else payload.get("blob")
            if (part not in PARTS or part not in parts_ok or content is None
                    or checksum_of(item_id, part, content) != r.get("checksum")):
                n_bad += 1
                continue
            cur = CONN.execute(
                "INSERT OR IGNORE INTO items(task_id,item_id,part,payload,checksum,"
                "collector_id,epoch,received_ts) VALUES(?,?,?,?,?,?,?,?)",
                (tid, item_id, part, json.dumps(payload), r.get("checksum"),
                 collector, epoch, now()))
            n_new += cur.rowcount
            n_dup += (1 - cur.rowcount)
        CONN.commit()
        record_submission(tid, epoch, collector, "batch", len(records),
                          n_new, n_dup, n_bad, "ok", "")
    send_json(h, 200, {"ok": True, "accepted": n_new, "duplicates": n_dup,
                       "bad_checksum": n_bad})


def _completeness(tid):
    t = get_task(tid)
    if not t:
        return None
    spec = json.loads(t["spec"])
    parts = spec.get("parts", ["detail"])
    expected_ids = {r["item_id"] for r in
                    CONN.execute("SELECT item_id FROM expected WHERE task_id=?", (tid,))}
    per_part = {}
    total_missing = 0
    for part in parts:
        got = {r["item_id"] for r in CONN.execute(
            "SELECT item_id FROM items WHERE task_id=? AND part=?", (tid, part))}
        missing = sorted(expected_ids - got)
        total_missing += len(missing)
        per_part[part] = {"expected": len(expected_ids), "received": len(got),
                          "missing": len(missing), "missing_sample": missing[:20]}
    agg = row(CONN, """SELECT COALESCE(SUM(n_dup),0) dup,
                              COALESCE(SUM(n_bad),0) bad,
                              COALESCE(SUM(CASE WHEN result='fenced' THEN 1 ELSE 0 END),0) fenced
                       FROM submissions WHERE task_id=?""", (tid,))
    expected_total = len(expected_ids) * len(parts)
    received_total = sum(v["received"] for v in per_part.values())
    return {"task_id": tid, "epoch": t["epoch"], "spec": spec,
            "discovery_done": bool(t["discovery_done"]),
            "expected_total": expected_total, "received_total": received_total,
            "missing_total": total_missing,
            "gap_ratio": (total_missing / expected_total) if expected_total else 0.0,
            "per_part": per_part, "duplicates": agg["dup"],
            "bad_checksum": agg["bad"], "fenced_submissions": agg["fenced"]}


def h_completeness(h, p, q):
    with LOCK:
        c = _completeness(int(q["task_id"]))
    if not c:
        send_json(h, 404, {"error": "unknown task"})
        return
    send_json(h, 200, c)


def h_missing(h, p, q):
    tid = int(q["task_id"])
    part = q.get("part", "detail")
    after = int(q.get("after", 0))
    limit = min(int(q.get("limit", 500)), 2000)
    with LOCK:
        expected = {r["item_id"] for r in CONN.execute(
            "SELECT item_id FROM expected WHERE task_id=?", (tid,))}
        got = {r["item_id"] for r in CONN.execute(
            "SELECT item_id FROM items WHERE task_id=? AND part=?", (tid, part))}
    missing = sorted(i for i in (expected - got) if i > after)[:limit]
    send_json(h, 200, {"task_id": tid, "part": part, "ids": missing})


def h_items(h, p, q):
    tid = int(q["task_id"])
    limit = min(int(q.get("limit", 200)), 2000)
    with LOCK:
        rs = rows(CONN, "SELECT item_id,part,epoch,collector_id,received_ts FROM items"
                        " WHERE task_id=? ORDER BY received_ts DESC LIMIT ?", (tid, limit))
    send_json(h, 200, {"rows": rs})


def h_stats(h, p, q):
    with LOCK:
        tids = [r["task_id"] for r in CONN.execute("SELECT task_id FROM tasks")]
        out = [_completeness(t) for t in tids]
    send_json(h, 200, {"tasks": out})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8100)
    ap.add_argument("--db", default="data/ingest.db")
    args = ap.parse_args()
    init_db(args.db)
    routes = [
        ("POST", "/task", h_task_upsert),
        ("POST", "/expected", h_expected),
        ("POST", "/batch", h_batch),
        ("GET", "/completeness", h_completeness),
        ("GET", "/missing", h_missing),
        ("GET", "/items", h_items),
        ("GET", "/stats", h_stats),
        ("GET", "/health", lambda h, p, q: send_json(h, 200, {"ok": True, "ts": now()})),
    ]
    serve(args.host, args.port, routes, "ingestor")


if __name__ == "__main__":
    main()
