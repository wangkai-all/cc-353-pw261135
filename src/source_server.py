#!/usr/bin/env python3
"""本地限速来源（模拟目标网站，端口默认 9000）。

三层令牌桶限速，一次请求必须同时通过：
  1. 接口层   每个接口（list/detail/image）一个桶
  2. 账号层   每个账号 token 一个桶
  3. 出口层   每个来源 IP 一个桶（采集进程用 127.0.0.x 区分出口）

任一一层不满足即返回 429。注意：Retry-After 只反映"等待最短的那一层"，
而 X-RateLimit-Wait-* 头分别给出三层各自的等待 —— 它们可能互相矛盾
（按 Retry-After 醒来的请求会撞在另一层上）。调度器必须取三层最大值。

管理接口（不限速、不计入请求日志）：
  GET  /admin/limits     当前限速配置 + 各桶实时等待
  PUT  /admin/limits     在线修改限速（模拟"来源临时改限速"）
  GET  /admin/requests   实际请求记录（地面真值，供调度器对账）
  POST /admin/reset      清空请求日志（测试用）
"""
import argparse
import json
import threading
import time

from common import (ACCOUNTS, CATALOG_SIZE, DEFAULT_SOURCE_LIMITS, INTERFACES,
                    PAGE_SIZE, catalog_ids, checksum_of, db, item_detail,
                    item_exists, item_image, now, read_json, rows, send_json,
                    serve)


class Bucket:
    def __init__(self, cap, rate):
        self.cap = float(cap)
        self.rate = float(rate)
        self.tokens = float(cap)
        self.updated = now()

    def _refill(self, t):
        self.tokens = min(self.cap, self.tokens + (t - self.updated) * self.rate)
        self.updated = t

    def wait(self, t):
        """还要等多少秒才能再取 1 个令牌（不消费）。"""
        self._refill(t)
        return 0.0 if self.tokens >= 1.0 else (1.0 - self.tokens) / self.rate

    def consume(self, t):
        self._refill(t)
        self.tokens -= 1.0

    def reconfigure(self, cap, rate):
        t = now()
        self._refill(t)
        self.cap, self.rate = float(cap), float(rate)
        self.tokens = min(self.tokens, self.cap)


class Source:
    def __init__(self, db_path, limits=None):
        self.lock = threading.Lock()
        self.conn = db(db_path)
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS requests(
                 id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, ip TEXT, account TEXT,
                 interface TEXT, path TEXT, status INTEGER, retry_after_ms INTEGER,
                 wait_if_ms INTEGER, wait_acct_ms INTEGER, wait_eg_ms INTEGER,
                 denied_by TEXT, permit_id TEXT, task_id TEXT, epoch TEXT)""")
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)")
        self.conn.commit()
        saved = self.conn.execute("SELECT v FROM kv WHERE k='limits'").fetchone()
        self.limits = limits or (json.loads(saved[0]) if saved else
                                 json.loads(json.dumps(DEFAULT_SOURCE_LIMITS)))
        self._save_limits()
        # 桶：接口桶预建；账号/出口桶按需创建
        self.if_buckets = {n: Bucket(**c) for n, c in self.limits["interfaces"].items()}
        self.acct_buckets = {}
        self.eg_buckets = {}

    def _save_limits(self):
        self.conn.execute("INSERT OR REPLACE INTO kv(k,v) VALUES('limits',?)",
                          (json.dumps(self.limits),))
        self.conn.commit()

    def _bucket(self, table, key, cfg):
        b = table.get(key)
        if b is None:
            b = table[key] = Bucket(cfg["cap"], cfg["rate"])
        return b

    def check(self, interface, account, ip):
        """返回 (ok, waits_ms_dict)。ok=True 时已消费三层令牌。"""
        t = now()
        with self.lock:
            ifb = self.if_buckets[interface]
            ab = self._bucket(self.acct_buckets, account, self.limits["account"])
            eb = self._bucket(self.eg_buckets, ip, self.limits["egress"])
            waits = {"interface": ifb.wait(t), "account": ab.wait(t), "egress": eb.wait(t)}
            ok = all(w <= 1e-9 for w in waits.values())
            if ok:
                ifb.consume(t)
                ab.consume(t)
                eb.consume(t)
            return ok, waits

    def log_request(self, ip, account, interface, path, status, waits, denied_by, hdrs):
        retry_ms = int(waits[denied_by] * 1000) if denied_by else 0
        with self.lock:
            self.conn.execute(
                "INSERT INTO requests(ts,ip,account,interface,path,status,retry_after_ms,"
                "wait_if_ms,wait_acct_ms,wait_eg_ms,denied_by,permit_id,task_id,epoch)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (now(), ip, account, interface, path, status, retry_ms,
                 int(waits["interface"] * 1000), int(waits["account"] * 1000),
                 int(waits["egress"] * 1000), denied_by or "",
                 hdrs.get("x-permit-id", ""), hdrs.get("x-task-id", ""),
                 hdrs.get("x-epoch", "")))
            self.conn.commit()


SRC = None  # 全局实例


def _deny_layer(waits):
    """429 时按'等待最短的阻塞层'给 Retry-After —— 与其他层更长的等待相矛盾。"""
    blocking = {k: w for k, w in waits.items() if w > 1e-9}
    if not blocking:
        return max(waits, key=lambda k: waits[k])
    return min(blocking, key=lambda k: blocking[k])


def handle_api(h, interface, q, hdrs):
    ip = h.client_address[0]
    auth = h.headers.get("Authorization", "")
    account = auth[7:] if auth.startswith("Bearer ") else ""
    path = h.path
    if account not in ACCOUNTS:
        SRC.log_request(ip, account, interface, path, 401,
                        {"interface": 0, "account": 0, "egress": 0}, "", hdrs)
        send_json(h, 401, {"error": "unknown account token"})
        return
    ok, waits = SRC.check(interface, account, ip)
    if not ok:
        denied = _deny_layer(waits)
        retry_ms = int(waits[denied] * 1000)
        SRC.log_request(ip, account, interface, path, 429, waits, denied, hdrs)
        send_json(h, 429, {
            "error": "rate_limited", "denied_by": denied,
            "retry_after_ms": retry_ms,
            "waits_ms": {k: int(v * 1000) for k, v in waits.items()},
            "hint": "Retry-After 只反映一层；三层等待可能互相矛盾，应取最大值",
        }, extra_headers={
            "Retry-After": str(max(1, int(waits[denied] + 0.999))),
            "X-RateLimit-Denied-By": denied,
            "X-RateLimit-Wait-Interface": str(int(waits["interface"] * 1000)),
            "X-RateLimit-Wait-Account": str(int(waits["account"] * 1000)),
            "X-RateLimit-Wait-Egress": str(int(waits["egress"] * 1000)),
        })
        return

    if interface == "list":
        id_from = int(q.get("from", 1))
        id_to = int(q.get("to", CATALOG_SIZE))
        page = int(q.get("page", 0))
        size = min(int(q.get("size", PAGE_SIZE)), 200)
        ids = catalog_ids(id_from, id_to)
        chunk = ids[page * size:(page + 1) * size]
        SRC.log_request(ip, account, interface, path, 200, waits, "", hdrs)
        send_json(h, 200, {"from": id_from, "to": id_to, "page": page, "size": size,
                           "total_ids": len(ids),
                           "pages": (len(ids) + size - 1) // size, "ids": chunk})
        return

    item_id = int(q.get("id", 0))
    if not (1 <= item_id <= CATALOG_SIZE) or not item_exists(item_id):
        SRC.log_request(ip, account, interface, path, 404, waits, "", hdrs)
        send_json(h, 404, {"error": "no such item", "id": item_id})
        return
    if interface == "detail":
        payload = item_detail(item_id)
        payload["checksum"] = checksum_of(item_id, "detail", payload["body"])
    else:  # image
        payload = item_image(item_id)
        payload["checksum"] = checksum_of(item_id, "image", payload["blob"])
    SRC.log_request(ip, account, interface, path, 200, waits, "", hdrs)
    send_json(h, 200, payload)


# ---------------------------------------------------------------- 路由

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--db", default="data/source.db")
    ap.add_argument("--config", help="JSON 文件，覆盖初始限速配置")
    args = ap.parse_args()

    global SRC
    limits = json.load(open(args.config)) if args.config else None
    SRC = Source(args.db, limits)

    def api_handler(h, p, q):
        interface = p["interface"]
        if interface not in INTERFACES:
            send_json(h, 404, {"error": "unknown interface"})
            return
        hdrs = {k.lower(): v for k, v in h.headers.items()}
        handle_api(h, interface, q, hdrs)

    def get_limits(h, p, q):
        t = now()
        with SRC.lock:
            buckets = {}
            for name, b in SRC.if_buckets.items():
                buckets[f"interface:{name}"] = {"tokens": round(b.tokens, 2),
                                                "wait_ms": int(b.wait(t) * 1000)}
            for name, b in SRC.acct_buckets.items():
                buckets[f"account:{name}"] = {"tokens": round(b.tokens, 2),
                                              "wait_ms": int(b.wait(t) * 1000)}
            for name, b in SRC.eg_buckets.items():
                buckets[f"egress:{name}"] = {"tokens": round(b.tokens, 2),
                                             "wait_ms": int(b.wait(t) * 1000)}
        send_json(h, 200, {"limits": SRC.limits, "buckets": buckets, "ts": t})

    def put_limits(h, p, q):
        body = read_json(h)
        with SRC.lock:
            for name, cfg in body.get("interfaces", {}).items():
                if name in SRC.limits["interfaces"]:
                    SRC.limits["interfaces"][name].update(
                        {k: float(v) for k, v in cfg.items() if k in ("cap", "rate")})
                    b = SRC.if_buckets[name]
                    b.reconfigure(**SRC.limits["interfaces"][name])
            for layer in ("account", "egress"):
                if layer in body:
                    SRC.limits[layer].update(
                        {k: float(v) for k, v in body[layer].items() if k in ("cap", "rate")})
                    table = SRC.acct_buckets if layer == "account" else SRC.eg_buckets
                    for b in table.values():
                        b.reconfigure(**SRC.limits[layer])
            SRC._save_limits()
        print(f"[source] limits updated: {json.dumps(body)}", flush=True)
        get_limits(h, p, q)

    def get_requests(h, p, q):
        since = int(q.get("since_id", 0))
        limit = min(int(q.get("limit", 500)), 2000)
        with SRC.lock:
            rs = rows(SRC.conn,
                      "SELECT * FROM requests WHERE id>? ORDER BY id LIMIT ?", (since, limit))
            max_id = SRC.conn.execute("SELECT COALESCE(MAX(id),0) FROM requests").fetchone()[0]
        send_json(h, 200, {"rows": rs, "max_id": max_id})

    def get_stats(h, p, q):
        with SRC.lock:
            by_status = rows(SRC.conn,
                             "SELECT status, COUNT(*) n FROM requests GROUP BY status")
            by_if = rows(SRC.conn,
                         "SELECT interface, status, COUNT(*) n FROM requests"
                         " GROUP BY interface, status")
        send_json(h, 200, {"by_status": by_status, "by_interface": by_if})

    def reset(h, p, q):
        with SRC.lock:
            SRC.conn.execute("DELETE FROM requests")
            SRC.conn.commit()
        send_json(h, 200, {"ok": True})

    routes = [
        ("GET", "/api/{interface}", api_handler),
        ("GET", "/admin/limits", get_limits),
        ("PUT", "/admin/limits", put_limits),
        ("GET", "/admin/requests", get_requests),
        ("GET", "/admin/stats", get_stats),
        ("POST", "/admin/reset", reset),
        ("GET", "/health", lambda h, p, q: send_json(h, 200, {"ok": True, "ts": now()})),
    ]
    serve(args.host, args.port, routes, "source")


if __name__ == "__main__":
    main()
