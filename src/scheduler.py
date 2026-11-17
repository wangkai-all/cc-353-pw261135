#!/usr/bin/env python3
"""统一调度服务（端口默认 8000）。

职责：
  - 任务分配与转移：每次转移 epoch+1（围栏令牌），先推进入库端再本地生效
  - 许可（permit）发放：采集进程每次请求来源前必须持有效许可；
    许可按 接口/账号/出口 三层分别建模 not_before（不是统一睡眠）
  - 可说明策略：公平分配 + 高优先级补缺（见 policy.py，决策明细落库）
  - 恢复核对：周期性对照来源的实际限速与实际请求记录，
    发现 未上报许可 / 无许可请求 / 过期许可被使用 等异常并记事件
  - Web UI 与 API 网关
"""
import argparse
import json
import os
import threading
import time

import policy
from common import (ACCOUNTS, CATALOG_SIZE, DEFAULT_SOURCE_LIMITS, INTERFACES,
                    POLICY_DEFAULTS, db, http_json, now, read_json, row, rows,
                    send_json, serve)

LOCK = threading.RLock()
CONN = None
EST = {}            # key -> earliest_ok 时刻（接口/账号/出口 三层估计）
POLICY_CFG = {}
ASSUMED = {}        # 调度器假设的来源限速（被对账循环校正）
GAP_CACHE = {}      # task_id -> 入库端完整性快照
RECENT = []         # [(ts, task_id)] 窗口内已发许可（公平项）
RECONCILE = {"limits_checked_ts": 0, "req_cursor": 0, "source_ok": False,
             "ingestor_ok": False}
STOP = False

SOURCE_URL = ""
INGEST_URL = ""
WEB_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "web", "index.html")


# ---------------------------------------------------------------- 基础

def event(etype, message, data=None):
    with LOCK:
        CONN.execute("INSERT INTO events(ts,type,message,data) VALUES(?,?,?,?)",
                     (now(), etype, message, json.dumps(data or {})))
        CONN.commit()
    print(f"[sched][{etype}] {message}", flush=True)


def kv_get(key, default=None):
    r = row(CONN, "SELECT v FROM kv WHERE k=?", (key,))
    return json.loads(r["v"]) if r else default


def kv_set(key, value):
    CONN.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)",
                 (key, json.dumps(value)))


def rate_of(key):
    layer, name = key.split(":", 1)
    if layer == "if":
        return ASSUMED["interfaces"][name]["rate"]
    return ASSUMED["account" if layer == "acct" else "egress"]["rate"]


def cap_of(key):
    layer, name = key.split(":", 1)
    if layer == "if":
        return ASSUMED["interfaces"][name]["cap"]
    return ASSUMED["account" if layer == "acct" else "egress"]["cap"]


def est_get(key, t):
    """令牌桶估计：最早可用时刻；按容量折算'满桶'下限，不累积超额信用。"""
    floor = t - (cap_of(key) - 1) / rate_of(key)
    return max(EST.get(key, floor), floor)


def est_set(key, ts):
    EST[key] = ts
    CONN.execute("INSERT OR REPLACE INTO estimates(key,earliest_ok,updated_ts)"
                 " VALUES(?,?,?)", (key, ts, now()))


def recent_count(task_id, t):
    global RECENT
    win = POLICY_CFG["window_sec"]
    RECENT[:] = [(ts, tid) for ts, tid in RECENT if t - ts < win]
    return sum(1 for _, tid in RECENT if tid == task_id)


def log_decision(cid, chosen, reason, candidates, expl):
    CONN.execute(
        "INSERT INTO decisions(ts,collector_id,reason,chosen,candidates) VALUES(?,?,?,?,?)",
        (now(), cid, reason,
         json.dumps(chosen, ensure_ascii=False) if chosen else "",
         json.dumps({"explain": expl, "candidates": candidates}, ensure_ascii=False)))
    CONN.commit()


# ---------------------------------------------------------------- 采集进程接口

def h_register(h, p, q):
    body = read_json(h)
    cid, egress = body.get("collector_id", ""), body.get("egress", "")
    if not cid:
        send_json(h, 400, {"error": "collector_id required"})
        return
    with LOCK:
        existing = row(CONN, "SELECT * FROM collectors WHERE id=?", (cid,))
        CONN.execute(
            "INSERT INTO collectors(id,egress,first_seen,last_heartbeat,status)"
            " VALUES(?,?,?,?,'online') ON CONFLICT(id) DO UPDATE SET"
            " egress=excluded.egress,last_heartbeat=excluded.last_heartbeat,status='online'",
            (cid, egress, now(), now()))
        CONN.commit()
    if not existing or existing["status"] != "online":
        event("collector-register", f"采集进程 {cid} 上线（出口 {egress}）")
    send_json(h, 200, {"ok": True, "heartbeat_ms": 2000})


def h_heartbeat(h, p, q):
    body = read_json(h)
    cid = body.get("collector_id", "")
    with LOCK:
        col = row(CONN, "SELECT * FROM collectors WHERE id=?", (cid,))
        if not col:
            send_json(h, 401, {"error": "unknown collector"})
            return
        CONN.execute("UPDATE collectors SET last_heartbeat=?,status='online',state=?"
                     " WHERE id=?",
                     (now(), json.dumps(body.get("tasks", [])), cid))
        tasks = rows(CONN, "SELECT * FROM tasks WHERE assigned=?", (cid,))
        CONN.commit()
    send_json(h, 200, {"now": now(), "tasks": [
        {"task_id": t["id"], "epoch": t["epoch"], "status": t["status"],
         "priority": t["priority"], "spec": json.loads(t["spec"])} for t in tasks]})


def h_permit_request(h, p, q):
    body = read_json(h)
    cid = body.get("collector_id", "")
    needs = body.get("needs", {})
    t = now()
    with LOCK:
        col = row(CONN, "SELECT * FROM collectors WHERE id=?", (cid,))
        if not col:
            send_json(h, 401, {"error": "unknown collector"})
            return
        egress = col["egress"]
        tasks = {r["id"]: r for r in rows(
            CONN, "SELECT * FROM tasks WHERE assigned=? AND status='active'", (cid,))}
        # ---- 围栏：needs 里的 epoch 必须全部等于当前值，否则整体 409
        current, stale = {}, False
        for tid_s, need in needs.items():
            tk = tasks.get(int(tid_s))
            if tk:
                current[int(tid_s)] = tk["epoch"]
                if int(need.get("epoch", -1)) != tk["epoch"]:
                    stale = True
        if stale:
            send_json(h, 409, {"error": "stale epoch", "current_epochs": current})
            return
        # ---- 组装候选 (任务 × 接口 × 账号)
        candidates = []
        for tid_s, need in needs.items():
            tid = int(tid_s)
            tk = tasks.get(tid)
            if not tk:
                continue
            gap = GAP_CACHE.get(tid, {}).get("gap_ratio", 0.0)
            recent = recent_count(tid, t)
            for iface in need.get("interfaces", []):
                if iface not in INTERFACES:
                    continue
                for acct in ACCOUNTS:
                    nb = max(est_get(f"if:{iface}", t), est_get(f"acct:{acct}", t),
                             est_get(f"eg:{egress}", t), t)
                    candidates.append({"task_id": tid, "interface": iface,
                                       "account": acct, "not_before": nb,
                                       "priority": tk["priority"],
                                       "gap_ratio": gap, "recent": recent})
        if not candidates:
            send_json(h, 200, {"wait_ms": 1000, "reason": "no-active-task"})
            return
        winner, reason, scored, wait = policy.choose(
            candidates, POLICY_CFG["gap_bonus"], POLICY_CFG["horizon_sec"], t)
        if winner is None:
            log_decision(cid, None, reason, [], "所有层都在冷却，暂无可行许可")
            send_json(h, 200, {"wait_ms": int(wait * 1000), "reason": reason})
            return
        # ---- 记账：三层各自推进一个令牌间隔（分层建模，非统一睡眠）
        for key in (f"if:{winner['interface']}", f"acct:{winner['account']}",
                    f"eg:{egress}"):
            est_set(key, est_get(key, t) + 1.0 / rate_of(key))
        expires = winner["not_before"] + POLICY_CFG["permit_ttl_sec"]
        cur = CONN.execute(
            "INSERT INTO permits(ts,task_id,epoch,collector_id,interface,account,egress,"
            "not_before,expires,status) VALUES(?,?,?,?,?,?,?,?,?,'issued')",
            (t, winner["task_id"], tasks[winner["task_id"]]["epoch"], cid,
             winner["interface"], winner["account"], egress,
             winner["not_before"], expires))
        RECENT.append((t, winner["task_id"]))
        winner_view = dict(winner)
        winner_view["not_before_ms"] = int((winner["not_before"] - t) * 1000)
        winner_view.pop("not_before", None)
        scored_view = []
        for c in scored[:8]:
            v = dict(c)
            v["not_before_ms"] = int((c["not_before"] - t) * 1000)
            v.pop("not_before", None)
            scored_view.append(v)
        expl = policy.explain(reason, winner_view, scored_view)
        log_decision(cid, winner_view, reason, scored_view, expl)
        CONN.commit()
        send_json(h, 200, {"permit": {
            "permit_id": cur.lastrowid, "task_id": winner["task_id"],
            "epoch": tasks[winner["task_id"]]["epoch"],
            "interface": winner["interface"], "account": winner["account"],
            "not_before": winner["not_before"], "expires": expires}, "reason": reason})


def h_permit_report(h, p, q):
    body = read_json(h)
    pid = int(body.get("permit_id", 0))
    outcome = body.get("outcome", "")
    t = now()
    with LOCK:
        perm = row(CONN, "SELECT * FROM permits WHERE id=?", (pid,))
        if not perm:
            send_json(h, 404, {"error": "unknown permit"})
            return
        new_status = {"ok": "used", "429": "used",
                      "error": "error", "void": "void"}.get(outcome, "used")
        CONN.execute("UPDATE permits SET status=?,outcome=?,report=?,report_ts=?"
                     " WHERE id=?",
                     (new_status, outcome, json.dumps(body), t, pid))
        keys = (f"if:{perm['interface']}", f"acct:{perm['account']}",
                f"eg:{perm['egress']}")
        if outcome == "429":
            # 用来源返回的三层等待校正估计（取各层各自的头，而非单一 Retry-After）
            waits = body.get("waits_ms", {})
            for layer, key in zip(("interface", "account", "egress"), keys):
                w = waits.get(layer)
                if w is not None:
                    est_set(key, max(est_get(key, t), t + float(w) / 1000.0))
        elif outcome == "void":
            for key in keys:
                est_set(key, max(est_get(key, t) - 1.0 / rate_of(key),
                                 t - (cap_of(key) - 1) / rate_of(key)))
        tk = row(CONN, "SELECT epoch FROM tasks WHERE id=?", (perm["task_id"],))
        epoch_ok = bool(tk) and int(body.get("epoch", -1)) == tk["epoch"]
        CONN.commit()
    if not epoch_ok:
        event("fenced-report",
              f"许可 {pid} 的回报携带过期 epoch（任务 {perm['task_id']}），已忽略其进度")
    send_json(h, 200, {"ok": True, "epoch_ok": epoch_ok})


# ---------------------------------------------------------------- 任务管理（UI）

def h_task_create(h, p, q):
    body = read_json(h)
    try:
        id_from, id_to = int(body["from"]), int(body["to"])
        priority = int(body.get("priority", 3))
        assigned = body["assigned"]
        with_images = bool(body.get("with_images"))
    except (KeyError, ValueError):
        send_json(h, 400, {"error": "from/to/priority/assigned 必填"})
        return
    if not (1 <= id_from <= id_to <= CATALOG_SIZE and 1 <= priority <= 5 and assigned):
        send_json(h, 400, {"error": f"范围需在 1..{CATALOG_SIZE} 内，优先级 1..5"})
        return
    spec = {"from": id_from, "to": id_to, "with_images": with_images,
            "parts": ["detail", "image"] if with_images else ["detail"]}
    with LOCK:
        cur = CONN.execute(
            "INSERT INTO tasks(spec,priority,status,assigned,epoch,created_ts,updated_ts)"
            " VALUES(?,?,'active',?,1,?,?)",
            (json.dumps(spec), priority, assigned, now(), now()))
        tid = cur.lastrowid
        CONN.commit()
    try:  # 先推进入库端，失败则回滚任务
        status, obj, _ = http_json("POST", INGEST_URL + "/task",
                                   {"task_id": tid, "spec": spec, "epoch": 1}, timeout=4)
        if status != 200:
            raise RuntimeError(obj)
    except Exception as exc:
        with LOCK:
            CONN.execute("DELETE FROM tasks WHERE id=?", (tid,))
            CONN.commit()
        send_json(h, 502, {"error": f"入库端不可达，任务未创建: {exc}"})
        return
    event("task-created",
          f"任务 {tid} 创建：{id_from}-{id_to} images={with_images} "
          f"优先级{priority} → {assigned}")
    send_json(h, 200, {"ok": True, "task_id": tid})


def h_task_transfer(h, p, q):
    tid = int(p["id"])
    target = read_json(h).get("target", "")
    if not target:
        send_json(h, 400, {"error": "target 必填"})
        return
    # 整个转移（推进入库端 + 本地生效）在同一把锁内完成：
    # 许可发放与 epoch 变更严格串行，转移后不可能再发出旧 epoch 的许可
    with LOCK:
        t = row(CONN, "SELECT * FROM tasks WHERE id=?", (tid,))
        if not t:
            send_json(h, 404, {"error": "unknown task"})
            return
        new_epoch = t["epoch"] + 1
        old, spec = t["assigned"], json.loads(t["spec"])
        err = None  # 关键顺序：先入库存端登记新 epoch，再本地生效
        for _ in range(3):
            try:
                status, obj, _ = http_json("POST", INGEST_URL + "/task",
                                           {"task_id": tid, "spec": spec,
                                            "epoch": new_epoch}, timeout=3)
                if status == 200:
                    break
                err = obj
            except Exception as exc:
                err = str(exc)
            time.sleep(0.3)
        else:
            send_json(h, 503, {"error": f"入库端不可达，转移已取消: {err}"})
            return
        CONN.execute("UPDATE tasks SET assigned=?,epoch=?,updated_ts=? WHERE id=?",
                     (target, new_epoch, now(), tid))
        CONN.commit()
    event("task-transferred",
          f"任务 {tid} 从 {old} 转移到 {target}，epoch {new_epoch - 1}→{new_epoch}；"
          f"旧进程将拿不到新许可、提交也会被入库端拒绝")
    send_json(h, 200, {"ok": True, "task_id": tid, "epoch": new_epoch})


def _task_update(h, p, field, value, etype, msg):
    with LOCK:
        t = row(CONN, "SELECT * FROM tasks WHERE id=?", (int(p["id"]),))
        if not t:
            send_json(h, 404, {"error": "unknown task"})
            return
        CONN.execute(f"UPDATE tasks SET {field}=?,updated_ts=? WHERE id=?",
                     (value, now(), int(p["id"])))
        CONN.commit()
    event(etype, msg)
    send_json(h, 200, {"ok": True})


def h_task_pause(h, p, q):
    _task_update(h, p, "status", "paused", "task-paused", f"任务 {p['id']} 暂停")


def h_task_resume(h, p, q):
    _task_update(h, p, "status", "active", "task-resumed", f"任务 {p['id']} 恢复")


def h_task_priority(h, p, q):
    prio = int(read_json(h).get("priority", 3))
    if not 1 <= prio <= 5:
        send_json(h, 400, {"error": "优先级 1..5"})
        return
    _task_update(h, p, "priority", prio, "priority-changed",
                 f"任务 {p['id']} 优先级调整为 {prio}")


# ---------------------------------------------------------------- UI API

def h_overview(h, p, q):
    t = now()
    with LOCK:
        collectors = rows(CONN, "SELECT * FROM collectors ORDER BY id")
        tasks = rows(CONN, "SELECT * FROM tasks ORDER BY id")
        events_ = rows(CONN, "SELECT * FROM events ORDER BY id DESC LIMIT 15")
        decisions = rows(CONN, "SELECT * FROM decisions ORDER BY id DESC LIMIT 8")
    for c in collectors:
        c["online"] = (t - c["last_heartbeat"]) < 8
        c.pop("state", None)
    for tk in tasks:
        tk["spec"] = json.loads(tk["spec"])
        tk["completeness"] = GAP_CACHE.get(tk["id"])
    for d in decisions:
        d["candidates"] = json.loads(d["candidates"]) if d["candidates"] else {}
        d["chosen"] = json.loads(d["chosen"]) if d["chosen"] else None
    send_json(h, 200, {"now": t, "collectors": collectors, "tasks": tasks,
                       "events": events_, "decisions": decisions,
                       "policy": POLICY_CFG, "assumed_limits": ASSUMED,
                       "reconcile": RECONCILE})


def h_events(h, p, q):
    limit = min(int(q.get("limit", 100)), 500)
    with LOCK:
        send_json(h, 200, {"rows": rows(
            CONN, "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))})


def h_decisions(h, p, q):
    limit = min(int(q.get("limit", 50)), 200)
    with LOCK:
        ds = rows(CONN, "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,))
    for d in ds:
        d["candidates"] = json.loads(d["candidates"]) if d["candidates"] else {}
        d["chosen"] = json.loads(d["chosen"]) if d["chosen"] else None
    send_json(h, 200, {"rows": ds})


def h_permits(h, p, q):
    limit = min(int(q.get("limit", 100)), 1000)
    with LOCK:
        send_json(h, 200, {"rows": rows(
            CONN, "SELECT * FROM permits ORDER BY id DESC LIMIT ?", (limit,))})


def h_policy_get(h, p, q):
    send_json(h, 200, dict(POLICY_CFG))


def h_policy_put(h, p, q):
    body = read_json(h)
    with LOCK:
        for k in POLICY_CFG:
            if k in body:
                POLICY_CFG[k] = float(body[k])
        kv_set("policy", POLICY_CFG)
        CONN.commit()
    event("policy-updated", f"策略参数调整：{json.dumps(body)}")
    send_json(h, 200, dict(POLICY_CFG))


def h_audit(h, p, q):
    with LOCK:
        by_status = rows(CONN, "SELECT status,COUNT(*) n FROM permits GROUP BY status")
        ev = lambda typ: row(CONN, "SELECT COUNT(*) n FROM events WHERE type=?",
                             (typ,))["n"]
        total = row(CONN, "SELECT COUNT(*) n FROM permits")["n"]
    send_json(h, 200, {
        "permits_total": total, "permits_by_status": by_status,
        "untracked_requests": ev("untracked-request"),
        "stale_permit_used": ev("stale-permit-used"),
        "used_unreported": ev("permit-used-unreported"),
        "fenced_reports": ev("fenced-report")})


def proxy(h, method, url, body=None):
    try:
        status, obj, _ = http_json(method, url, body=body, timeout=4)
        send_json(h, status, obj)
    except Exception as exc:
        send_json(h, 502, {"error": f"上游不可达: {exc}"})


def h_index(h, p, q):
    try:
        data = open(WEB_HTML, "rb").read()
    except OSError:
        send_json(h, 404, {"error": "web/index.html missing"})
        return
    h.send_response(200)
    h.send_header("Content-Type", "text/html; charset=utf-8")
    h.send_header("Content-Length", str(len(data)))
    h.end_headers()
    h.wfile.write(data)


# ---------------------------------------------------------------- 对账循环

def reconcile_limits():
    """对照来源当前限速：修正假设 + 用来源各桶实时等待校准估计。"""
    global ASSUMED
    try:
        status, obj, _ = http_json("GET", SOURCE_URL + "/admin/limits", timeout=3)
        if status != 200:
            raise RuntimeError(obj)
    except Exception:
        RECONCILE["source_ok"] = False
        return
    RECONCILE["source_ok"] = True
    RECONCILE["limits_checked_ts"] = now()
    actual = obj["limits"]
    with LOCK:
        if actual != ASSUMED:
            diff = _diff_limits(ASSUMED, actual)
            ASSUMED = actual
            kv_set("assumed_limits", ASSUMED)
            CONN.commit()
            event("limits-changed", f"检测到来源限速变化（周期核对）: {diff}", diff)
        t = now()
        for key, b in obj.get("buckets", {}).items():
            layer, name = key.split(":", 1)
            est_key = {"interface": "if", "account": "acct",
                       "egress": "eg"}[layer] + ":" + name
            if est_key.startswith("if:") and name not in ASSUMED["interfaces"]:
                continue
            est_set(est_key, max(est_get(est_key, t),
                                 t + b["wait_ms"] / 1000.0))
        CONN.commit()


def _diff_limits(old, new):
    out = {}

    def walk(prefix, a, b):
        if isinstance(b, dict):
            for k, v in b.items():
                walk(f"{prefix}.{k}" if prefix else k,
                     a.get(k, {}) if isinstance(a, dict) else {}, v)
        elif a != b:
            out[prefix] = {"old": a, "new": b}

    walk("", old, new)
    return out


def reconcile_requests():
    """对照来源实际请求记录：每个请求都必须能对应到已发许可。"""
    try:
        status, obj, _ = http_json(
            "GET", f"{SOURCE_URL}/admin/requests?since_id={RECONCILE['req_cursor']}"
                   "&limit=1000", timeout=3)
        if status != 200:
            raise RuntimeError(obj)
    except Exception:
        RECONCILE["source_ok"] = False
        return
    RECONCILE["source_ok"] = True
    unreported = 0
    with LOCK:
        for r in obj["rows"]:
            RECONCILE["req_cursor"] = max(RECONCILE["req_cursor"], r["id"])
            pid = (r["permit_id"] or "").strip()
            perm = row(CONN, "SELECT * FROM permits WHERE id=?",
                       (int(pid),)) if pid.isdigit() else None
            if not perm:
                event("untracked-request",
                      f"来源记录到无许可请求: {r['interface']} {r['account']} "
                      f"from {r['ip']} status={r['status']}", r)
                continue
            if perm["status"] == "issued":
                CONN.execute("UPDATE permits SET status='used-unreported' WHERE id=?",
                             (perm["id"],))
                unreported += 1
            tk = row(CONN, "SELECT epoch FROM tasks WHERE id=?", (perm["task_id"],))
            if tk and perm["epoch"] < tk["epoch"]:
                event("stale-permit-used",
                      f"告警：过期许可 {perm['id']}（任务 {perm['task_id']} "
                      f"epoch {perm['epoch']}<{tk['epoch']}）仍消耗了来源额度", r)
        kv_set("req_cursor", RECONCILE["req_cursor"])
        CONN.commit()
    if unreported:
        event("permit-used-unreported",
              f"{unreported} 个许可在来源端有请求记录但采集进程未回报"
              f"（进程可能在请求成功后退出），已按已消耗记账")


def refresh_completeness():
    with LOCK:
        tids = [r["id"] for r in rows(
            CONN, "SELECT id FROM tasks WHERE status IN ('active','paused')")]
    for tid in tids:
        try:
            status, obj, _ = http_json(
                "GET", f"{INGEST_URL}/completeness?task_id={tid}", timeout=3)
            if status != 200:
                continue
            RECONCILE["ingestor_ok"] = True
        except Exception:
            RECONCILE["ingestor_ok"] = False
            continue
        with LOCK:
            GAP_CACHE[tid] = obj
            tk = row(CONN, "SELECT status FROM tasks WHERE id=?", (tid,))
            if (tk and tk["status"] == "active" and obj.get("discovery_done")
                    and obj.get("missing_total") == 0):
                CONN.execute("UPDATE tasks SET status='done',updated_ts=? WHERE id=?",
                             (now(), tid))
                CONN.commit()
                event("task-complete",
                      f"任务 {tid} 完成：应收 {obj['expected_total']} "
                      f"实收 {obj['received_total']} 重复 {obj['duplicates']} "
                      f"校验和错误 {obj['bad_checksum']}")


def janitor():
    t = now()
    with LOCK:
        lost = CONN.execute(
            "UPDATE permits SET status='lost' WHERE status='issued' AND expires<?",
            (t - 2.0,)).rowcount
        CONN.commit()
        offline = rows(CONN,
                       "SELECT id FROM collectors WHERE status='online'"
                       " AND last_heartbeat<?", (t - 15,))
        for c in offline:
            CONN.execute("UPDATE collectors SET status='offline' WHERE id=?", (c["id"],))
        CONN.commit()
    if lost:
        event("permit-lost", f"{lost} 个许可超时未回报，按已消耗保守记账")
    for c in offline:
        event("collector-offline", f"采集进程 {c['id']} 失联")


def loop(fn, interval):
    while not STOP:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            print(f"[sched] loop {fn.__name__} error: {exc}", flush=True)
        time.sleep(interval)


# ---------------------------------------------------------------- 启动

def init_db(path):
    global CONN
    CONN = db(path)
    CONN.executescript("""
    CREATE TABLE IF NOT EXISTS tasks(
      id INTEGER PRIMARY KEY AUTOINCREMENT, spec TEXT, priority INTEGER,
      status TEXT, assigned TEXT, epoch INTEGER, created_ts REAL, updated_ts REAL);
    CREATE TABLE IF NOT EXISTS collectors(
      id TEXT PRIMARY KEY, egress TEXT, first_seen REAL, last_heartbeat REAL,
      status TEXT, state TEXT);
    CREATE TABLE IF NOT EXISTS permits(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, task_id INTEGER, epoch INTEGER,
      collector_id TEXT, interface TEXT, account TEXT, egress TEXT,
      not_before REAL, expires REAL, status TEXT, outcome TEXT, report TEXT,
      report_ts REAL);
    CREATE TABLE IF NOT EXISTS decisions(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, collector_id TEXT,
      reason TEXT, chosen TEXT, candidates TEXT);
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, type TEXT, message TEXT,
      data TEXT);
    CREATE TABLE IF NOT EXISTS estimates(
      key TEXT PRIMARY KEY, earliest_ok REAL, updated_ts REAL);
    CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
    """)
    CONN.commit()


def load_state():
    global POLICY_CFG, ASSUMED
    POLICY_CFG = kv_get("policy", dict(POLICY_DEFAULTS))
    ASSUMED = kv_get("assumed_limits",
                     json.loads(json.dumps(DEFAULT_SOURCE_LIMITS)))
    RECONCILE["req_cursor"] = kv_get("req_cursor", 0)
    for r in rows(CONN, "SELECT * FROM estimates"):
        EST[r["key"]] = r["earliest_ok"]


def seed_tasks():
    with LOCK:
        n = row(CONN, "SELECT COUNT(*) n FROM tasks")["n"]
    if n:
        return
    for _ in range(30):  # 等入库端就绪
        try:
            s, _, _ = http_json("GET", INGEST_URL + "/health", timeout=2)
            if s == 200:
                break
        except Exception:
            pass
        time.sleep(1)
    seeds = [
        ({"from": 1, "to": 150, "with_images": True,
          "parts": ["detail", "image"]}, 3, "A"),
        ({"from": 151, "to": 260, "with_images": False,
          "parts": ["detail"]}, 2, "B"),
    ]
    for spec, prio, who in seeds:
        with LOCK:
            cur = CONN.execute(
                "INSERT INTO tasks(spec,priority,status,assigned,epoch,created_ts,"
                "updated_ts) VALUES(?,?,'active',?,1,?,?)",
                (json.dumps(spec), prio, who, now(), now()))
            tid = cur.lastrowid
            CONN.commit()
        try:
            http_json("POST", INGEST_URL + "/task",
                      {"task_id": tid, "spec": spec, "epoch": 1}, timeout=4)
            event("task-created",
                  f"种子任务 {tid}：{spec['from']}-{spec['to']} → {who}")
        except Exception as exc:
            event("seed-failed", f"种子任务 {tid} 推送入库端失败: {exc}")


def main():
    global SOURCE_URL, INGEST_URL, STOP
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", default="data/scheduler.db")
    ap.add_argument("--source", default="http://127.0.0.1:9000")
    ap.add_argument("--ingest", default="http://127.0.0.1:8100")
    ap.add_argument("--seed", action="store_true", help="首次启动时创建演示任务")
    args = ap.parse_args()
    SOURCE_URL, INGEST_URL = args.source, args.ingest

    init_db(args.db)
    load_state()
    with LOCK:
        n_tasks = row(CONN, "SELECT COUNT(*) n FROM tasks")["n"]
        n_permits = row(CONN, "SELECT COUNT(*) n FROM permits")["n"]
    event("scheduler-start",
          f"调度器启动：{n_tasks} 个任务、{n_permits} 个历史许可已恢复，开始核对")

    if args.seed:
        threading.Thread(target=seed_tasks, daemon=True).start()
    threading.Thread(target=loop, args=(reconcile_limits, 5), daemon=True).start()
    threading.Thread(target=loop, args=(reconcile_requests, 4), daemon=True).start()
    threading.Thread(target=loop, args=(refresh_completeness, 3), daemon=True).start()
    threading.Thread(target=loop, args=(janitor, 2), daemon=True).start()

    routes = [
        ("POST", "/api/register", h_register),
        ("POST", "/api/heartbeat", h_heartbeat),
        ("POST", "/api/permit/request", h_permit_request),
        ("POST", "/api/permit/report", h_permit_report),
        ("GET", "/", h_index),
        ("GET", "/index.html", h_index),
        ("GET", "/ui/api/overview", h_overview),
        ("GET", "/ui/api/events", h_events),
        ("GET", "/ui/api/decisions", h_decisions),
        ("GET", "/ui/api/permits", h_permits),
        ("GET", "/ui/api/audit", h_audit),
        ("GET", "/ui/api/policy", h_policy_get),
        ("PUT", "/ui/api/policy", h_policy_put),
        ("POST", "/ui/api/tasks", h_task_create),
        ("POST", "/ui/api/tasks/{id}/transfer", h_task_transfer),
        ("POST", "/ui/api/tasks/{id}/priority", h_task_priority),
        ("POST", "/ui/api/tasks/{id}/pause", h_task_pause),
        ("POST", "/ui/api/tasks/{id}/resume", h_task_resume),
        ("POST", "/ui/api/reconcile", lambda h, p, q: (
            reconcile_limits(), reconcile_requests(),
            send_json(h, 200, {"ok": True, "reconcile": RECONCILE}))),
        ("GET", "/ui/api/source/limits",
         lambda h, p, q: proxy(h, "GET", SOURCE_URL + "/admin/limits")),
        ("PUT", "/ui/api/source/limits",
         lambda h, p, q: proxy(h, "PUT", SOURCE_URL + "/admin/limits", read_json(h))),
        ("GET", "/ui/api/source/requests",
         lambda h, p, q: proxy(h, "GET", SOURCE_URL + "/admin/requests?since_id="
                               + q.get("since_id", "0") + "&limit="
                               + q.get("limit", "100"))),
        ("GET", "/ui/api/ingest/completeness",
         lambda h, p, q: proxy(h, "GET", INGEST_URL + "/completeness?task_id="
                               + q.get("task_id", ""))),
        ("GET", "/ui/api/ingest/stats",
         lambda h, p, q: proxy(h, "GET", INGEST_URL + "/stats")),
        ("GET", "/health", lambda h, p, q: send_json(h, 200, {"ok": True, "ts": now()})),
    ]
    serve(args.host, args.port, routes, "scheduler")


if __name__ == "__main__":
    main()
