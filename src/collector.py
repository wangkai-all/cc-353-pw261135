#!/usr/bin/env python3
"""采集进程（运行两个实例，各自绑定独立出口 IP，如 127.0.0.1 / 127.0.0.2）。

纪律：
  - 没有调度器许可绝不向来源发请求 —— 任务转移后旧进程拿不到新许可，
    自然无法继续消耗同一份额度
  - 每条成功响应先写本地 WAL 再上报入库端；进程在"请求成功后退出"，
    重启时重放 WAL，入库端幂等去重
  - 心跳发现 epoch 过期/任务被转移 → 立即放弃本地状态；
    缓存数据尝试提交，被入库端 409 围栏则丢弃
  - 调度器短暂离线：指数退避等待，绝不绕过许可
"""
import argparse
import json
import signal
import time
from collections import deque

from common import PAGE_SIZE, db, http_json, now


class Collector:
    def __init__(self, cid, egress, sched, ingest, source, db_path):
        self.cid, self.egress = cid, egress
        self.sched, self.ingest, self.source = sched, ingest, source
        self.conn = db(db_path)
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS wal(
                 id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER, epoch INTEGER,
                 item_id INTEGER, part TEXT, payload TEXT, checksum TEXT,
                 fetched_ts REAL, submitted INTEGER DEFAULT 0)""")
        self.conn.commit()
        self.tasks = {}       # tid -> 本地任务状态
        self.stop = False
        self.last_hb = 0.0
        self.last_flush = 0.0

    def log(self, msg):
        print(f"[collector-{self.cid}] {msg}", flush=True)

    def post(self, base, path, body, timeout=5.0):
        return http_json("POST", base + path, body, timeout=timeout)

    def interruptible_sleep(self, sec):
        deadline = now() + sec
        while not self.stop and now() < deadline:
            time.sleep(min(0.1, deadline - now()))

    # ---------------------------------------------------------- 注册/心跳

    def register(self):
        while not self.stop:
            try:
                s, _, _ = self.post(self.sched, "/api/register",
                                    {"collector_id": self.cid, "egress": self.egress})
                if s == 200:
                    self.log("已注册")
                    return
            except Exception as exc:
                self.log(f"注册重试: {exc}")
            time.sleep(1)

    def heartbeat(self):
        body = {"collector_id": self.cid, "egress": self.egress, "tasks": [
            {"task_id": tid, "epoch": st["epoch"],
             "queued": sum(len(q) for q in st["queues"].values()),
             "wal_pending": self.wal_pending()}
            for tid, st in self.tasks.items()]}
        s, o, _ = self.post(self.sched, "/api/heartbeat", body)
        if s == 401:
            self.register()
            return True
        if s != 200:
            return False
        self.sync_tasks(o.get("tasks", []))
        return True

    def sync_tasks(self, task_list):
        current = {}
        for t in task_list:
            tid = t["task_id"]
            current[tid] = t
            st = self.tasks.get(tid)
            if st is None or st["epoch"] != t["epoch"]:
                if st:
                    self.log(f"任务 {tid} epoch {st['epoch']}→{t['epoch']}，重新加载")
                self.load_task(t)
            else:
                st["status"], st["spec"] = t["status"], t["spec"]
        for tid in list(self.tasks):
            if tid not in current:
                self.log(f"任务 {tid} 已转移/撤销，放弃本地状态")
                self.flush_wal(only_task=tid)
                del self.tasks[tid]

    def load_task(self, t):
        tid, spec = t["task_id"], t["spec"]
        st = {"task_id": tid, "epoch": t["epoch"], "status": t["status"], "spec": spec,
              "discovery_done": False, "next_page": 0,
              "queues": {"detail": deque(), "image": deque()}}
        expected_ids = 0
        try:
            s, comp, _ = http_json("GET", f"{self.ingest}/completeness?task_id={tid}",
                                   timeout=4)
            if s == 200:
                st["discovery_done"] = comp["discovery_done"]
                expected_ids = comp["per_part"].get("detail", {}).get("expected", 0)
        except Exception as exc:
            self.log(f"任务 {tid} 完整性查询失败: {exc}")
        # 已发现的 id 按入库缺失集合恢复队列，已入库的绝不重复抓取
        self.resync_queues(st)
        if not st["discovery_done"] and expected_ids:
            # 发现阶段中断：list 页对确定性目录是可重放的，
            # 已推入 expected_ids 个 → 从下一页继续，而不是从头再来
            st["next_page"] = expected_ids // PAGE_SIZE
            self.log(f"任务 {tid} 续 discovery：从第 {st['next_page']} 页开始")
        self.tasks[tid] = st
        self.log(f"任务 {tid} 载入 epoch={t['epoch']} "
                 f"discovery_done={st['discovery_done']} "
                 f"队列 detail={len(st['queues']['detail'])} "
                 f"image={len(st['queues']['image'])}")

    def resync_queues(self, st):
        for part in ("detail", "image"):
            if part not in st["spec"].get("parts", ["detail"]):
                continue
            try:
                s, o, _ = http_json(
                    "GET", f"{self.ingest}/missing?task_id={st['task_id']}"
                           f"&part={part}&limit=2000", timeout=4)
                if s == 200:
                    st["queues"][part] = deque(o["ids"])
            except Exception as exc:
                self.log(f"任务 {st['task_id']} 缺失集合同步失败: {exc}")

    # ---------------------------------------------------------- 许可执行

    def compute_needs(self):
        needs = {}
        for tid, st in self.tasks.items():
            if st["status"] != "active":
                continue
            ifaces = []
            if not st["discovery_done"]:
                ifaces.append("list")
            ifaces += [pt for pt in ("detail", "image") if st["queues"][pt]]
            if not ifaces:
                self.resync_queues(st)
                ifaces = [pt for pt in ("detail", "image") if st["queues"][pt]]
            if ifaces:
                needs[str(tid)] = {"epoch": st["epoch"], "interfaces": ifaces}
        return needs

    def execute(self, permit):
        tid, iface = permit["task_id"], permit["interface"]
        st = self.tasks.get(tid)
        if not st or st["epoch"] != permit["epoch"]:
            self.report(permit, "void", note="task revoked locally")
            return
        item_id = None
        if iface == "list":
            spec = st["spec"]
            path = (f"/api/list?from={spec['from']}&to={spec['to']}"
                    f"&page={st['next_page']}&size={PAGE_SIZE}")
        else:
            if not st["queues"][iface]:
                self.report(permit, "void", note="queue empty")
                return
            item_id = st["queues"][iface].popleft()
            path = f"/api/{iface}?id={item_id}"
        headers = {"Authorization": f"Bearer {permit['account']}",
                   "X-Permit-Id": str(permit["permit_id"]),
                   "X-Task-Id": str(tid), "X-Epoch": str(permit["epoch"])}
        t0 = now()
        try:
            status, obj, hdrs = http_json(
                "GET", self.source + path, headers=headers, timeout=10,
                source_address=(self.egress, 0))
        except Exception as exc:
            if item_id is not None:
                st["queues"][iface].appendleft(item_id)
            self.report(permit, "error", note=str(exc))
            time.sleep(1)
            return
        latency = int((now() - t0) * 1000)
        if status == 200:
            if iface == "list":
                ids = obj["ids"]
                st["queues"]["detail"].extend(ids)
                if st["spec"].get("with_images"):
                    st["queues"]["image"].extend(ids)
                st["next_page"] += 1
                final = st["next_page"] >= obj["pages"]
                if self.push_expected(tid, st["epoch"], ids, final) and final:
                    st["discovery_done"] = True
                    self.log(f"任务 {tid} 发现完成：{obj['total_ids']} 个 item")
            else:
                payload = {"id": obj["id"]}
                if iface == "detail":
                    payload.update({"title": obj["title"], "body": obj["body"]})
                else:
                    payload["blob"] = obj["blob"]
                self.wal_append(tid, permit["epoch"], item_id, iface,
                                payload, obj["checksum"])
            self.report(permit, "ok", status=200, latency_ms=latency)
        elif status == 429:
            if item_id is not None:
                st["queues"][iface].appendleft(item_id)
            waits = {"interface": int(hdrs.get("x-ratelimit-wait-interface", 0)),
                     "account": int(hdrs.get("x-ratelimit-wait-account", 0)),
                     "egress": int(hdrs.get("x-ratelimit-wait-egress", 0))}
            self.report(permit, "429", status=429, waits_ms=waits,
                        latency_ms=latency)
        else:
            if item_id is not None:
                st["queues"][iface].appendleft(item_id)
            self.report(permit, "error", status=status, latency_ms=latency)

    def push_expected(self, tid, epoch, ids, final):
        try:
            s, o, _ = self.post(self.ingest, "/expected",
                                {"task_id": tid, "epoch": epoch,
                                 "collector_id": self.cid,
                                 "item_ids": ids, "final": final}, timeout=6)
        except Exception as exc:
            self.log(f"expected 上报失败: {exc}")
            return False
        if s == 409:
            self.log(f"任务 {tid} 被围栏（epoch {epoch}），放弃")
            self.tasks.pop(tid, None)
            return False
        return s == 200

    def report(self, permit, outcome, **extra):
        body = {"permit_id": permit["permit_id"], "task_id": permit["task_id"],
                "epoch": permit["epoch"], "outcome": outcome}
        body.update(extra)
        try:
            s, o, _ = self.post(self.sched, "/api/permit/report", body)
            if s == 200 and not o.get("epoch_ok", True):
                self.log(f"任务 {permit['task_id']} 回报被围栏，放弃本地状态")
                self.tasks.pop(permit["task_id"], None)
        except Exception as exc:
            self.log(f"许可 {permit['permit_id']} 回报失败（调度器将对账补记）: {exc}")

    # ---------------------------------------------------------- WAL

    def wal_append(self, tid, epoch, item_id, part, payload, checksum):
        self.conn.execute(
            "INSERT INTO wal(task_id,epoch,item_id,part,payload,checksum,fetched_ts)"
            " VALUES(?,?,?,?,?,?,?)",
            (tid, epoch, item_id, part, json.dumps(payload), checksum, now()))
        self.conn.commit()

    def wal_pending(self):
        return self.conn.execute(
            "SELECT COUNT(*) FROM wal WHERE submitted=0").fetchone()[0]

    def flush_wal(self, only_task=None):
        sql = "SELECT * FROM wal WHERE submitted=0"
        args = ()
        if only_task:
            sql += " AND task_id=?"
            args = (only_task,)
        groups = {}
        for r in self.conn.execute(sql + " ORDER BY id LIMIT 100", args).fetchall():
            groups.setdefault((r["task_id"], r["epoch"]), []).append(r)
        for (tid, epoch), rs in groups.items():
            records = [{"item_id": r["item_id"], "part": r["part"],
                        "payload": json.loads(r["payload"]), "checksum": r["checksum"]}
                       for r in rs]
            try:
                s, o, _ = self.post(self.ingest, "/batch",
                                    {"task_id": tid, "epoch": epoch,
                                     "collector_id": self.cid, "records": records},
                                    timeout=8)
            except Exception as exc:
                self.log(f"WAL 提交失败（稍后重试）: {exc}")
                return
            ids = [(r["id"],) for r in rs]
            if s == 200:
                self.conn.executemany("UPDATE wal SET submitted=1 WHERE id=?", ids)
                self.conn.commit()
            elif s == 409:
                self.conn.executemany("DELETE FROM wal WHERE id=?", ids)
                self.conn.commit()
                self.log(f"任务 {tid} epoch {epoch} 的 {len(ids)} 条缓存被围栏丢弃")
            else:
                self.log(f"WAL 提交异常 {s}: {o}")
                return

    # ---------------------------------------------------------- 主循环

    def run(self):
        self.register()
        while not self.stop:
            try:
                if self.heartbeat():
                    break
            except Exception as exc:
                self.log(f"心跳重试: {exc}")
            time.sleep(1)
        if self.wal_pending():
            self.log(f"启动重放 WAL：{self.wal_pending()} 条未提交记录")
            self.flush_wal()
        backoff = 0.5
        while not self.stop:
            try:
                if now() - self.last_hb > 2:
                    if not self.heartbeat():
                        raise RuntimeError("heartbeat rejected")
                    self.last_hb = now()
                pending = self.wal_pending()
                if pending >= 8 or (pending and now() - self.last_flush > 2):
                    self.flush_wal()
                    self.last_flush = now()
                needs = self.compute_needs()
                if not needs:
                    time.sleep(0.5)
                    continue
                s, o, _ = self.post(self.sched, "/api/permit/request",
                                    {"collector_id": self.cid, "needs": needs})
                if s == 409:
                    self.log("许可被拒：epoch 过期，立即重新同步")
                    self.last_hb = 0
                    continue
                if s == 401:
                    self.register()
                    continue
                if s != 200:
                    time.sleep(1)
                    continue
                permit = o.get("permit")
                if not permit:
                    self.interruptible_sleep(min(o.get("wait_ms", 1000) / 1000, 2.0))
                    continue
                self.log(f"获得许可 {permit['permit_id']} task={permit['task_id']} "
                         f"{permit['interface']}@{permit['account']} "
                         f"not_before={permit['not_before']:.3f}")
                delay = permit["not_before"] - now()
                if delay > 0:
                    self.interruptible_sleep(min(delay, 10))
                self.execute(permit)
                backoff = 0.5
            except Exception as exc:
                self.log(f"调度器不可达或内部错误: {exc}；{backoff:.1f}s 后重试")
                self.interruptible_sleep(backoff)
                backoff = min(backoff * 2, 5.0)
        self.log("退出前最后一次 WAL 提交")
        try:
            self.flush_wal()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--egress", required=True, help="本进程出口 IP，如 127.0.0.1")
    ap.add_argument("--scheduler", default="http://127.0.0.1:8000")
    ap.add_argument("--ingest", default="http://127.0.0.1:8100")
    ap.add_argument("--source", default="http://127.0.0.1:9000")
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    c = Collector(args.id, args.egress, args.scheduler, args.ingest,
                  args.source, args.db)

    def _stop(*_):
        c.stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    c.run()


if __name__ == "__main__":
    main()
