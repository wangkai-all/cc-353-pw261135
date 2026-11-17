"""共享基础设施：端口、账号、限速默认值、目录数据、HTTP/JSON 小框架、SQLite 助手。

整个演示只依赖 Python 标准库。所有组件都在本机运行：
  source_server.py  : 本地限速来源（模拟目标网站，三层令牌桶）
  scheduler.py      : 统一调度服务（任务/租约/许可/对账 + Web UI）
  ingestor.py       : 入库端（幂等入库、完整性、围栏校验）
  collector.py      : 采集进程（两个实例，各自独立出口 IP）
"""
import hashlib
import http.client
import json
import sqlite3
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ---------------------------------------------------------------- 常量

SOURCE_PORT = 9000
SCHED_PORT = 8000
INGEST_PORT = 8100

ACCOUNTS = ["acct-a", "acct-b", "acct-c"]

CATALOG_SIZE = 600          # 来源目录里的 item id 范围: 1..600（稀疏存在）
PAGE_SIZE = 50              # /api/list 每页

# 来源三层限速的默认配置（令牌桶 capacity / 每秒补充速率）
DEFAULT_SOURCE_LIMITS = {
    "interfaces": {
        "list":   {"cap": 2.0, "rate": 0.5},
        "detail": {"cap": 4.0, "rate": 1.5},
        "image":  {"cap": 6.0, "rate": 3.0},
    },
    "account": {"cap": 5.0, "rate": 1.0},   # 每个账号
    "egress":  {"cap": 8.0, "rate": 2.0},   # 每个出口 IP
}

# 调度策略默认值（网页可在线调整）
POLICY_DEFAULTS = {
    "gap_bonus": 2.0,        # 缺口加权系数：有效权重 = 优先级 * (1 + gap_bonus * 缺口率)
    "horizon_sec": 6.0,      # 许可调度视野：not_before 超过 now+horizon 的候选不发许可
    "permit_ttl_sec": 4.0,   # 许可有效期（任务转移后旧许可最多泄漏 ttl 时长）
    "window_sec": 30.0,      # 公平性统计窗口（近期已发许可数）
}

INTERFACES = ("list", "detail", "image")
PARTS = ("detail", "image")


def now():
    return time.time()


def ms(ts):
    return int(round(ts * 1000))


# ---------------------------------------------------------------- 目录数据（确定性生成）

def item_exists(item_id):
    """稀疏目录：约 3/4 的 id 真实存在，由 id 确定性决定。"""
    return hashlib.sha256(f"exists-{item_id}".encode()).digest()[0] % 4 != 0


def catalog_ids(id_from, id_to):
    return [i for i in range(id_from, id_to + 1) if item_exists(i)]


def item_detail(item_id):
    body = "body-" + hashlib.sha256(f"body-{item_id}".encode()).hexdigest()[:24]
    return {"id": item_id, "title": f"Item {item_id}", "body": body}


def item_image(item_id):
    blob = hashlib.sha256(f"img-{item_id}".encode()).hexdigest()
    return {"id": item_id, "blob": blob}


def checksum_of(item_id, part, content):
    """入库端据此重算校验和，验证数据完整性。"""
    return hashlib.sha256(f"{item_id}:{part}:{content}".encode()).hexdigest()


# ---------------------------------------------------------------- HTTP 客户端

def http_json(method, url, body=None, timeout=5.0, headers=None, source_address=None):
    """返回 (status, obj, headers_dict)。headers 键统一小写。网络异常向上抛。"""
    u = urlparse(url)
    conn = http.client.HTTPConnection(
        u.hostname, u.port or 80, timeout=timeout, source_address=source_address)
    path = u.path + ("?" + u.query if u.query else "")
    hdrs = {}
    if body is not None:
        hdrs["Content-Type"] = "application/json"
    if headers:
        hdrs.update(headers)
    conn.request(method, path,
                 body=json.dumps(body).encode() if body is not None else None,
                 headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read()
    out_headers = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    try:
        obj = json.loads(raw.decode()) if raw else {}
    except ValueError:
        obj = {"_raw": raw.decode(errors="replace")}
    return resp.status, obj, out_headers


# ---------------------------------------------------------------- HTTP 服务端小框架

def send_json(h, status, obj, extra_headers=None):
    data = json.dumps(obj).encode()
    h.send_response(status)
    h.send_header("Content-Type", "application/json")
    h.send_header("Content-Length", str(len(data)))
    if extra_headers:
        for k, v in extra_headers.items():
            h.send_header(k, str(v))
    h.end_headers()
    h.wfile.write(data)


def read_json(h):
    length = int(h.headers.get("Content-Length") or 0)
    if not length:
        return {}
    return json.loads(h.rfile.read(length).decode())


def _route_match(pattern, path):
    p = pattern.strip("/").split("/")
    s = path.strip("/").split("/")
    if len(p) != len(s):
        return None
    params = {}
    for a, b in zip(p, s):
        if a.startswith("{") and a.endswith("}"):
            params[a[1:-1]] = b
        elif a != b:
            return None
    return params


def serve(host, port, routes, name):
    """routes: [(method, pattern, handler)]，handler(h, params, query)。"""
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self, method):
            u = urlparse(self.path)
            query = {k: v[0] for k, v in parse_qs(u.query).items()}
            for m, pattern, fn in routes:
                if m != method:
                    continue
                params = _route_match(pattern, u.path)
                if params is None:
                    continue
                try:
                    fn(self, params, query)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as exc:  # noqa: BLE001
                    traceback.print_exc()
                    try:
                        send_json(self, 500, {"error": str(exc)})
                    except Exception:
                        pass
                return
            send_json(self, 404, {"error": "not found", "path": u.path})

        do_GET = lambda s: s._handle("GET")     # noqa: E731
        do_POST = lambda s: s._handle("POST")   # noqa: E731
        do_PUT = lambda s: s._handle("PUT")     # noqa: E731

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    print(f"[{name}] listening on {host}:{port}", flush=True)
    httpd.serve_forever()


# ---------------------------------------------------------------- SQLite

def db(path):
    # check_same_thread=False：连接跨线程使用，各服务自行用锁串行化访问
    conn = sqlite3.connect(path, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def row(conn, sql, args=()):
    r = conn.execute(sql, args).fetchone()
    return dict(r) if r else None
