"""W0/W1 调度受理、幂等领取与窗口回执的端到端场景校验（直接驱动领域服务）。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.store import store  # noqa: E402
from app.services import service_for  # noqa: E402


def run() -> None:
    sched = service_for("window_scheduler")
    compute = service_for("metric_compute")

    mc = store.rows("metric_catalog")
    mc.append({"id": max(int(r["id"]) for r in mc) + 1, "status": "已生效", "pending": False,
               "abnormal": False, "指标编码": "METR-X", "指标名称": "测试指标",
               "计算口径": "sum(x)", "聚合窗口": "日窗口", "口径版本": "v1"})
    ws = store.rows("window_scheduler")
    pid = max(int(r["id"]) for r in ws) + 1
    ws.append({"id": pid, "status": "待执行", "pending": True, "abnormal": False,
               "计划名称": "测试滚动计划", "指标编码": "METR-X", "窗口粒度": "日窗口",
               "下次窗口": "2026-11-25T00:00:00+00:00", "上次状态": "待执行"})

    # W1 受理 + 回执丢失重放：同一作业、同一回执
    r1 = sched.trigger_window(pid)
    w1_job = r1["job"]["id"]
    rcp_no = r1["receipt"]["回执编号"]
    assert r1["ok"] and not r1["idempotent"]
    assert sched.trigger_window(pid)["job"]["id"] == w1_job
    assert len([j for j in store.rows("metric_compute") if j.get("调度计划ID") == pid]) == 1

    # W1 首次领取：W0 从未受理 -> 阻断，能核对是等待 W0
    claim_w1 = compute.claim_job(w1_job)
    assert not claim_w1["ok"] and claim_w1.get("waiting_window") == "2026-11-24"
    gap = sched.window_gap_report(pid)
    assert any(g["缺失窗口"] == "2026-11-24" for g in gap["gaps"])

    # 回退 W0：W1 已受理作业不被改写
    assert sched.rollback_window(pid, "2026-11-24T00:00:00+00:00")["ok"]
    assert store.find("metric_compute", w1_job)["status"] == "待执行"
    r0 = sched.trigger_window(pid)
    w0_job = r0["job"]["id"]
    assert w0_job != w1_job and r0["job"]["窗口键"] == "2026-11-24"

    # W0 可领取（回退窗口豁免前序缺口）；W0 在执行中时 W1 也能领取
    cn = compute.claim_next_for_plan(pid)
    assert cn["ok"] and cn["job"]["id"] == w0_job
    assert compute.claim_job(w0_job)["idempotent"]
    assert compute.claim_job(w1_job)["ok"]
    assert store.find("metric_compute", w1_job)["status"] == "执行中"

    # 完成两窗；回执确认且调度时版次可回查
    assert compute.start_job(w0_job)["ok"]
    assert compute.start_job(w1_job)["ok"]
    rcp_w1 = sched.get_receipt(rcp_no)
    assert rcp_w1["receipt"]["回执状态"] == "已确认"
    assert rcp_w1["receipt"]["口径版本"] == "v1"
    assert rcp_w1["receipt"]["确认时间"] is not None

    # 同一回执编号在调度侧与作业批次侧都能找回同一次计算
    rlist = sched.list_receipts(pid, "2026-11-25")
    blist = compute.list_batches(pid, "2026-11-25")
    assert len(rlist["receipts"]) == 1 and rlist["receipts"][0]["回执编号"] == rcp_no
    assert len(blist["batches"]) == 1 and blist["batches"][0]["回执编号"] == rcp_no
    assert blist["batches"][0]["作业ID"] == w1_job

    # 补齐后无缺段、两窗位置正确
    gap2 = sched.window_gap_report(pid)
    assert gap2["gaps"] == []
    assert [w["窗口键"] for w in gap2["windows"]] == ["2026-11-24", "2026-11-25"]

    # 作业页核查窗口归属与回退来源
    insp = compute.inspect_job_window(w0_job)
    assert insp["job"]["回退来源"] == "2026-11-25"
    assert insp["job"]["作业类型"] == "补算"
    assert insp["receipt"] is not None

    # 切换指标版次 v2：同窗口产生独立可辨认补算，v1 已确认结果保留
    for m in store.rows("metric_catalog"):
        if m.get("指标编码") == "METR-X" and m.get("status") == "已生效":
            m["status"] = "已废弃"
            m["pending"] = False
    store.rows("metric_catalog").append({
        "id": 999, "status": "已生效", "pending": False, "abnormal": False,
        "指标编码": "METR-X", "指标名称": "测试指标", "计算口径": "sum(x)/n",
        "聚合窗口": "日窗口", "口径版本": "v2",
    })
    assert sched.rollback_window(pid, "2026-11-25T00:00:00+00:00")["ok"]
    r3 = sched.trigger_window(pid)
    assert r3["job"]["id"] != w1_job
    assert r3["job"]["口径版本"] == "v2"
    assert r3["job"]["作业类型"] == "补算"
    assert store.find("metric_compute", w1_job)["status"] == "已完成"
    assert sched.get_receipt(rcp_no)["receipt"]["口径版本"] == "v1"
    # 同版次同窗口只有一份可执行作业
    assert sched.trigger_window(pid)["job"]["id"] == r3["job"]["id"]
    same_window = [j for j in store.rows("metric_compute")
                   if j.get("调度计划ID") == pid and j["窗口键"] == "2026-11-25"]
    assert len(same_window) == 2 and {j["口径版本"] for j in same_window} == {"v1", "v2"}

    # 通用动作入口：立即执行走幂等受理；启动作业走领取+启动
    p = store.find("window_scheduler", pid)
    p["status"] = "待执行"
    p["下次窗口"] = "2026-11-26T00:00:00+00:00"
    _, msg = sched.run_action(pid, "立即执行")
    assert "已受理" in msg
    new_job = [j for j in store.rows("metric_compute") if j.get("窗口键") == "2026-11-26"][0]
    _, msg2 = compute.run_action(new_job["id"], "启动作业")
    assert store.find("metric_compute", new_job["id"])["status"] == "已完成", msg2

    # 批量入口
    bt = sched.batch_trigger_due()
    assert bt["ok"] and "幂等去重" in bt["message"]
    be = compute.batch_execute_pending()
    assert be["ok"]

    # 异常路径
    assert sched.trigger_window(99999)["ok"] is False
    assert compute.claim_job(99999)["ok"] is False
    assert sched.rollback_window(pid, "2026-12-01T00:00:00+00:00")["ok"] is False
    assert sched.list_receipts(99999)["ok"] is False
    assert sched.get_receipt("NOPE")["ok"] is False
    p2 = store.find("window_scheduler", pid)
    p2["status"] = "执行中"
    assert sched.pause_plan(pid, "人工暂停")["ok"]
    assert store.find("window_scheduler", pid)["status"] == "待执行"

    print("batch_trigger:", bt["message"])
    print("batch_execute:", be["message"])
    print("ALL SCENARIO ASSERTIONS PASSED")


if __name__ == "__main__":
    run()
