"""许可分配策略（纯函数，便于测试与解释）。

目标：在"公平分配"与"高优先级补缺"之间形成可说明的策略。

每个候选 = (任务, 接口, 账号) 三元组，其 not_before 取三层估计的最大值：
    not_before = max(接口层可用时刻, 账号层可用时刻, 出口层可用时刻, now)
即调度器按层分别建模，而不是给所有请求统一睡眠。

候选评分：
    有效权重 = 优先级 * (1 + gap_bonus * 缺口率)      # 缺口率 = 缺失/应收
    得分     = 有效权重 / (1 + 窗口内该任务已获许可数)  # 公平项：近期拿得越多分越低

决策理由（reason）：
  - "fair-share"：赢家仅凭基础优先级+公平项胜出；
  - "gap-fill"   ：存在基础优先级更高的可行候选，但赢家靠缺口加分反超。
每次决策把全部候选的评分明细写入 decisions 表，网页可查看。
"""


def score_task(priority, gap_ratio, recent_grants, gap_bonus):
    eff = priority * (1.0 + gap_bonus * gap_ratio)
    return eff / (1.0 + recent_grants), eff


def choose(candidates, gap_bonus, horizon_sec, now):
    """candidates: [{task_id, interface, account, not_before, priority, gap_ratio, recent}]
    返回 (winner, reason, scored_candidates, wait_ms)。无可行候选时 winner=None。
    """
    feasible = [c for c in candidates if c["not_before"] <= now + horizon_sec]
    scored = []
    for c in feasible:
        s, eff = score_task(c["priority"], c["gap_ratio"], c["recent"], gap_bonus)
        scored.append({**c, "score": round(s, 4), "eff_weight": round(eff, 4)})
    if not scored:
        wait = min((c["not_before"] for c in candidates), default=now + 1.0) - now
        return None, "all-layers-cooling", [], max(0.05, wait)

    scored.sort(key=lambda c: (-c["score"], c["not_before"]))
    winner = scored[0]

    # 判定理由：是否存在"基础优先级严格更高"的候选被赢家靠缺口加分压过
    higher_prio_beaten = any(
        c["task_id"] != winner["task_id"] and c["priority"] > winner["priority"]
        for c in scored[1:])
    if higher_prio_beaten and winner["gap_ratio"] > 0.01:
        reason = "gap-fill"
    else:
        reason = "fair-share"
    return winner, reason, scored, 0.0


def explain(reason, winner, scored):
    """生成人类可读的决策说明。"""
    if winner is None:
        return "所有层都在冷却，暂无可行许可"
    w = winner
    base = (f"任务{w['task_id']} {w['interface']}@{w['account']} "
            f"not_before+{w['not_before_ms']}ms 得分{w['score']}"
            f"(优先级{w['priority']} 缺口{w['gap_ratio']:.0%} 近期{w['recent']}次)")
    if reason == "gap-fill":
        beaten = [c for c in scored[1:]
                  if c["task_id"] != w["task_id"] and c["priority"] > w["priority"]]
        if beaten:
            b = beaten[0]
            base += (f"；补缺反超任务{b['task_id']}(优先级{b['priority']} "
                     f"缺口{b['gap_ratio']:.0%} 得分{b['score']})")
    return base
