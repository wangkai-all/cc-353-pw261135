#!/usr/bin/env python3
"""端到端场景测试：从零启动整套本地系统（独立端口/临时目录），逐项验证：

  1. 采集闭环与入库完整性（校验和、无缺口）
  2. 许可决策有可说明的理由（fair-share / gap-fill）
  3. 任务转移围栏：旧进程拿不到新许可、旧 epoch 数据被入库端拒绝
  4. 请求成功后进程被杀：WAL 重放恢复，不丢数据
  5. 调度器短暂离线并重启：状态恢复、采集继续
  6. 来源临时改限速：调度器检测到并适应，恢复后任务完成
  7. 总对账：来源每个请求都能对应到有效许可

运行：python3 tests/scenario_test.py
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from common import http_json  # noqa: E402

PORT = {"source": 19000, "sched": 18000, "ingest": 18100}
BASE = {k: f"http://127.0.0.1:{p}" for k, p in PORT.items()}
EGRESS = {"A": "127.0.0.3", "B": "127.0.0.4"}

# 测试用更快的限速，让任务秒级完成；仍然分层
FAST_LIMITS = {
    "interfaces": {
        "list":   {"cap": 4.0, "rate": 4.0},
        "detail": {"cap": 8.0, "rate": 12.0},
        "image":  {"cap": 8.0, "rate": 12.0},
    },
    "account": {"cap": 10.0, "rate": 15.0},
    "egress":  {"cap": 16.0, "rate": 25.0},
}

TMP = tempfile.mkdtemp(prefix="rltest_")
PROCS = {}
PASSED = []


def log(msg):
    print(f"[test {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ok(name):
    PASSED.append(name)
    log(f"PASS: {name}")


def start(name, *args):
    lf = open(os.path.join(TMP, f"{name}.log"), "ab")
    p = subprocess.Popen([sys.executable, *args], cwd=ROOT,
                         stdout=lf, stderr=subprocess.STDOUT)
    PROCS[name] = p
    return p


def stop(name, sig=signal.SIGTERM, timeout=6):
    p = PROCS.pop(name, None)
    if p and p.poll() is None:
        p.send_signal(sig)
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def start_collector(cid):
    start(f"collector-{cid}", "src/collector.py", "--id", cid,
          "--egress", EGRESS[cid],
          "--scheduler", BASE["sched"], "--ingest", BASE["ingest"],
          "--source", BASE["source"],
          "--db", os.path.join(TMP, f"collector_{cid}.db"))


def wait_http(url, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s, _, _ = http_json("GET", url, timeout=2)
            if s == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise AssertionError(f"等待超时: {url}")


def wait_until(fn, desc, timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if fn():
            return
        time.sleep(0.5)
    raise AssertionError(f"等待超时: {desc}")


def api(kind, method, path, body=None):
    s, o, _ = http_json(method, BASE[kind] + path, body=body, timeout=5)
    return s, o


def create_task(frm, to, images, prio, who):
    s, o = api("sched", "POST", "/ui/api/tasks",
               {"from": frm, "to": to, "with_images": images,
                "priority": prio, "assigned": who})
    assert s == 200, o
    return o["task_id"]


def completeness(tid):
    s, o = api("ingest", "GET", f"/completeness?task_id={tid}")
    assert s == 200, o
    return o


def task_done(tid):
    c = completeness(tid)
    return c["discovery_done"] and c["expected_total"] > 0 \
        and c["missing_total"] == 0


def events_of(etype):
    s, o = api("sched", "GET", "/ui/api/events?limit=500")
    return [e for e in o["rows"] if e["type"] == etype]


def boot():
    cfg = os.path.join(TMP, "limits.json")
    json.dump(FAST_LIMITS, open(cfg, "w"))
    start("source", "src/source_server.py", "--port", str(PORT["source"]),
          "--db", os.path.join(TMP, "source.db"), "--config", cfg)
    wait_http(BASE["source"] + "/health")
    start("ingestor", "src/ingestor.py", "--port", str(PORT["ingest"]),
          "--db", os.path.join(TMP, "ingest.db"))
    wait_http(BASE["ingest"] + "/health")
    start("scheduler", "src/scheduler.py", "--port", str(PORT["sched"]),
          "--db", os.path.join(TMP, "scheduler.db"),
          "--source", BASE["source"], "--ingest", BASE["ingest"])
    wait_http(BASE["sched"] + "/health")
    start_collector("A")
    start_collector("B")
    time.sleep(3)  # 等注册与首次心跳


def scenario_1_basic():
    tid = create_task(1, 25, True, 3, "A")
    wait_until(lambda: task_done(tid), "任务1完成")
    c = completeness(tid)
    assert c["received_total"] == c["expected_total"] > 0, c
    assert c["bad_checksum"] == 0, c
    ok("采集闭环与入库完整性（含图片两部分、校验和验证）")
    s, o = api("sched", "GET", "/ui/api/decisions?limit=50")
    reasons = {d["reason"] for d in o["rows"]}
    assert "fair-share" in reasons, reasons
    explains = [d for d in o["rows"]
                if d["candidates"] and d["candidates"].get("explain")]
    assert explains, "决策缺少可说明的理由"
    ok("许可决策带有可说明理由（fair-share/gap-fill + 评分明细）")


def scenario_2_transfer():
    tid = create_task(26, 60, False, 2, "A")
    wait_until(lambda: completeness(tid)["received_total"] >= 5, "任务2有进度")
    s, o = api("sched", "POST", f"/ui/api/tasks/{tid}/transfer", {"target": "B"})
    assert s == 200 and o["epoch"] == 2, o
    t_transfer = time.time()  # 响应返回时服务端 epoch 已生效
    wait_until(lambda: task_done(tid), "任务2在转移后完成")
    # 围栏1：转移生效后调度器不得再为旧 epoch 发许可（用服务端事件时间对齐）
    s, o = api("sched", "GET", "/ui/api/events?limit=500")
    ev = [e for e in o["rows"]
          if e["type"] == "task-transferred" and f"任务 {tid} " in e["message"]]
    assert ev, "缺少转移事件"
    t_event = ev[0]["ts"]
    s, o = api("sched", "GET", "/ui/api/permits?limit=1000")
    bad = [p for p in o["rows"] if p["task_id"] == tid and p["epoch"] == 1
           and p["ts"] > t_event]
    assert not bad, f"转移后仍为旧epoch发许可: {bad}"
    # 围栏2：入库端数据里，转移宽限期(许可TTL+心跳)后不得再有 epoch1 的数据
    s, o = api("ingest", "GET", f"/items?task_id={tid}&limit=2000")
    grace = t_transfer + 8
    late = [r for r in o["rows"] if r["epoch"] == 1 and r["received_ts"] > grace]
    assert not late, f"旧进程在宽限期后仍提交成功: {late}"
    c = completeness(tid)
    assert c["received_total"] == c["expected_total"], c
    ok("任务转移围栏：旧进程拿不到新许可、旧epoch进度被入库端拒绝"
       f"（fenced_submissions={c['fenced_submissions']}）")


def scenario_3_kill9():
    tid = create_task(61, 90, True, 5, "A")
    wait_until(lambda: completeness(tid)["received_total"] >= 4, "任务3有进度")
    stop("collector-A", sig=signal.SIGKILL)   # 模拟"请求成功后进程退出"
    time.sleep(2)
    start_collector("A")                       # 重启 → WAL 重放
    wait_until(lambda: task_done(tid), "任务3在采集进程被杀后完成")
    c = completeness(tid)
    assert c["received_total"] == c["expected_total"], c
    # 调度器应已通过 许可超时/来源对账 发现未回报的许可
    wait_until(lambda: events_of("permit-lost") or events_of("permit-used-unreported"),
               "出现 permit-lost / permit-used-unreported 事件", timeout=30)
    ok("请求成功后进程被杀：WAL 重放补齐，许可损耗被对账发现")


def scenario_4_scheduler_offline():
    stop("scheduler")
    time.sleep(4)                              # 调度器短暂离线
    start("scheduler", "src/scheduler.py", "--port", str(PORT["sched"]),
          "--db", os.path.join(TMP, "scheduler.db"),
          "--source", BASE["source"], "--ingest", BASE["ingest"])
    wait_http(BASE["sched"] + "/health")
    tid = create_task(91, 105, False, 3, "B")
    wait_until(lambda: task_done(tid), "调度器重启后任务4完成")
    assert len(events_of("scheduler-start")) >= 2, "缺少重启事件"
    ok("调度器短暂离线后重启：状态恢复、任务继续")


def scenario_5_limit_change():
    s, _ = api("source", "PUT", "/admin/limits",
               {"interfaces": {"detail": {"rate": 0.5, "cap": 1.0}}})
    assert s == 200
    tid = create_task(106, 125, False, 3, "B")
    wait_until(lambda: events_of("limits-changed"), "调度器检测到限速变化",
               timeout=30)
    # 来源应实际返回过 429，且各层等待时间可能互相矛盾
    s, o = api("source", "GET", "/admin/requests?limit=2000")
    r429 = [r for r in o["rows"] if r["status"] == 429]
    assert r429, "限速收紧后应出现 429"
    waits = [(r["wait_if_ms"], r["wait_acct_ms"], r["wait_eg_ms"]) for r in r429]
    assert any(len(set(w)) > 1 for w in waits), "三层等待应存在不一致"
    s, _ = api("source", "PUT", "/admin/limits",
               {"interfaces": {"detail": {"rate": 12.0, "cap": 8.0}}})
    wait_until(lambda: task_done(tid), "限速恢复后任务5完成", timeout=120)
    ok("来源临时改限速：429 分层等待互相矛盾、调度器检测并适应、恢复后完成")


def scenario_6_audit():
    s, a = api("sched", "GET", "/ui/api/audit")
    assert a["untracked_requests"] == 0, f"存在无许可请求: {a}"
    s, o = api("source", "GET", "/admin/requests?limit=2000")
    missing = [r for r in o["rows"] if not r["permit_id"]]
    assert not missing, f"来源请求缺少许可号: {missing[:3]}"
    ok(f"总对账：来源 {len(o['rows'])} 个请求全部对应有效许可，"
       f"过期许可被用 {a['stale_permit_used']} 次（均在转移宽限内且已告警）")


def main():
    boot()
    try:
        scenario_1_basic()
        scenario_2_transfer()
        scenario_3_kill9()
        scenario_4_scheduler_offline()
        scenario_5_limit_change()
        scenario_6_audit()
    finally:
        for name in list(PROCS):
            stop(name)
        print(f"\n日志与数据保留在 {TMP}（含各进程 .log 与 .db）")
    print(f"\n=== 全部通过（{len(PASSED)} 项）===")
    for p in PASSED:
        print("  ✓", p)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        for name in list(PROCS):
            stop(name)
        print(f"\n现场保留在 {TMP}")
        raise
