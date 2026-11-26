"""窗口调度器领域服务：管理指标窗口的滚动调度、粒度控制与执行状态回写。

数据链路位置：调度驱动层，按指标口径的聚合窗口驱动计算作业滚动执行，并把
每次窗口的执行结果同步给数据时效监控判定新鲜度。

关键不变量：
- 同一调度计划、同一指标口径版本、同一计算窗口只有一份可执行作业；重复触发
  （含确认回执丢失后的重放）返回同一份窗口作业回执，不再分派第二份作业。
- 回退只拨窗口指针并登记回退来源，不会改写此前窗口已受理/执行中/已确认的作业。
- 每个窗口受理都生成可查回的窗口作业回执，调度侧与指标作业侧用同一回执编号
  定位同一次有效计算。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


# 窗口粒度 -> 步进（天），用于推导连续窗口的前序窗口键与缺口。
_GRANULARITY_STEP_DAYS = {
    "日窗口": 1,
    "周窗口": 7,
    "月窗口": 30,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _window_key(granularity: str, watermark: str) -> str:
    """把窗口水位归一为窗口键：日粒度取日期段，其余保留原始水位值。"""
    text = str(watermark or "").strip()
    if not text:
        return ""
    if granularity == "日窗口" and len(text) >= 10:
        return text[:10]
    return text


def _previous_window_key(granularity: str, window_key: str) -> str:
    """推导同粒度下紧邻的前序窗口键，无法推导时返回空串。"""
    if not window_key:
        return ""
    if granularity == "日窗口" and len(window_key) == 10:
        try:
            current = datetime.strptime(window_key, "%Y-%m-%d")
        except ValueError:
            return ""
        from datetime import timedelta

        return (current - timedelta(days=1)).strftime("%Y-%m-%d")
    if granularity == "周窗口" and len(window_key) == 10:
        try:
            current = datetime.strptime(window_key, "%Y-%m-%d")
        except ValueError:
            return ""
        from datetime import timedelta

        return (current - timedelta(days=7)).strftime("%Y-%m-%d")
    if granularity == "月窗口" and len(window_key) == 10:
        try:
            current = datetime.strptime(window_key, "%Y-%m-%d")
        except ValueError:
            return ""
        from datetime import timedelta

        return (current - timedelta(days=30)).strftime("%Y-%m-%d")
    return ""


def _next_window_key(granularity: str, window_key: str) -> str:
    """推导同粒度下紧邻的后序窗口键，无法推导时返回空串。"""
    if not window_key:
        return ""
    from datetime import timedelta

    step = _GRANULARITY_STEP_DAYS.get(granularity)
    if step and len(window_key) == 10:
        try:
            current = datetime.strptime(window_key, "%Y-%m-%d")
        except ValueError:
            return ""
        return (current + timedelta(days=step)).strftime("%Y-%m-%d")
    return ""


class WindowSchedulerService(BaseModuleService):
    MODULE = "window_scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["计划名称", "指标编码", "窗口粒度", "下次窗口", "上次状态", "回退来源"]
    REQUIRED_FIELDS = ["计划名称", "指标编码", "窗口粒度"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
    # 「立即执行」由 trigger_window 走带幂等受理的领域逻辑，而不是通用骨架直接改状态。
    ACTION_RULES = {"暂停计划": "待执行"}
    NEGATIVE_ACTIONS = []

    # ---- 回执与作业检索 ----

    @staticmethod
    def _receipt_rows() -> list[dict[str, Any]]:
        return store.rows("window_dispatch_receipt")

    def _find_receipt(self, plan_id: int, metric_version: str, window_key: str) -> dict[str, Any] | None:
        """按 计划 + 口径版次 + 窗口 的幂等键找回已登记的回执。"""
        for receipt in self._receipt_rows():
            if (
                int(receipt.get("调度计划ID", 0)) == plan_id
                and str(receipt.get("口径版本", "")) == metric_version
                and str(receipt.get("窗口键", "")) == window_key
            ):
                return receipt
        return None

    def _find_job(self, plan_id: int, metric_version: str, window_key: str) -> dict[str, Any] | None:
        for job in store.rows("metric_compute"):
            if (
                int(job.get("调度计划ID", 0)) == plan_id
                and str(job.get("口径版本", "")) == metric_version
                and str(job.get("窗口键", "")) == window_key
            ):
                return job
        return None

    @staticmethod
    def _effective_metric(metric_code: str) -> dict[str, Any] | None:
        return next(
            (m for m in store.rows("metric_catalog")
             if m.get("指标编码") == metric_code and m.get("status") == "已生效"),
            None,
        )

    # ---- 业务扩展：窗口调度与状态回写 ----

    def trigger_window(self, entry_id: int) -> dict[str, Any]:
        """触发一次窗口调度（幂等受理）。

        同一计划/口径版次/窗口重复触发时，直接返回此前已受理的窗口作业回执，
        不会再分派第二份作业——这同时覆盖「确认回执丢失后重放」的场景。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "调度计划不存在或已归档"}
        if entry.get("status") not in ("待执行", "执行中"):
            return {"ok": False, "message": f"当前状态「{entry.get('status')}」不可触发窗口"}

        metric_code = str(entry.get("指标编码", ""))
        metric = self._effective_metric(metric_code)
        if metric is None:
            entry["status"] = "执行失败"
            entry["上次状态"] = "执行失败"
            entry["abnormal"] = True
            return {"ok": False, "message": f"口径 {metric_code} 未生效，调度失败"}

        granularity = str(entry.get("窗口粒度", "日窗口"))
        watermark = str(entry.get("下次窗口", "")) or _now_iso()
        window_key = _window_key(granularity, watermark)
        metric_version = str(metric.get("口径版本", ""))

        # 幂等受理：回执已存在（无论确认是否丢失），原样返回同一作业。
        existing_receipt = self._find_receipt(entry_id, metric_version, window_key)
        if existing_receipt is not None:
            existing_job = store.find("metric_compute", int(existing_receipt.get("作业ID", 0)))
            entry["status"] = "执行中"
            entry["上次状态"] = "已受理"
            entry["pending"] = True
            return {
                "ok": True,
                "message": (
                    f"窗口 {window_key} 已受理（回执 {existing_receipt['回执编号']}），"
                    "重复触发未再分派作业"
                ),
                "receipt": dict(existing_receipt),
                "job": dict(existing_job) if existing_job else None,
                "idempotent": True,
            }

        # 同窗口存在其它版次的补算作业是允许的；同版次同窗口仍有在途作业则拒绝再建。
        active_job = self._find_job(entry_id, metric_version, window_key)
        if active_job is not None and active_job.get("status") != "已完成":
            return {
                "ok": False,
                "message": (
                    f"窗口 {window_key} 已有在途作业（作业 {active_job.get('id')}），"
                    "禁止重复分派"
                ),
            }

        compute_rows = store.rows("metric_compute")
        job_id = max((int(r.get("id", 0)) for r in compute_rows), default=0) + 1
        # 回执编号含口径版次，保证同窗口切换版次补算时各版次回执全局唯一、可辨认。
        receipt_no = f"RCP-{entry_id}-{window_key}-{metric_version}"
        job: dict[str, Any] = {
            "id": job_id,
            "作业名称": f"{entry.get('计划名称', '调度')}窗口作业-{window_key}",
            "指标编码": metric_code,
            "口径版本": metric_version,
            "计算窗口": granularity,
            "窗口键": window_key,
            "窗口水位": watermark,
            "调度计划ID": entry_id,
            "回执编号": receipt_no,
            "回退来源": str(entry.get("回退来源", "")),
            "作业类型": "补算" if entry.get("回退来源") else "常规",
            "作业状态": "待执行",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
        }
        compute_rows.append(job)

        receipt = {
            "id": max((int(r.get("id", 0)) for r in self._receipt_rows()), default=0) + 1,
            "回执编号": receipt_no,
            "调度计划ID": entry_id,
            "计划名称": entry.get("计划名称", ""),
            "指标编码": metric_code,
            "口径版本": metric_version,
            "计算窗口": granularity,
            "窗口键": window_key,
            "窗口水位": watermark,
            "作业ID": job_id,
            "受理时间": _now_iso(),
            "确认时间": None,
            "回退来源": str(entry.get("回退来源", "")),
            "回执状态": "已受理",
            "status": "已受理",
            "pending": True,
            "abnormal": False,
        }
        self._receipt_rows().append(receipt)

        entry["status"] = "执行中"
        entry["上次状态"] = "已受理"
        entry["pending"] = True
        entry["当前窗口键"] = window_key
        # 首次受理窗口作为连续日报锚点：缺口分析只在锚点的紧邻前序查一层，
        # 避免回退补齐后沿时间轴无限向前误报缺口。
        if not entry.get("首窗锚点"):
            entry["首窗锚点"] = window_key
        return {
            "ok": True,
            "message": f"窗口 {window_key} 已受理，作业 {job_id} 已创建（回执 {receipt_no}）",
            "receipt": dict(receipt),
            "job": dict(job),
            "idempotent": False,
        }

    def pause_plan(self, entry_id: int, reason: str) -> dict[str, Any]:
        """暂停调度计划并通知数据时效监控进入延迟观察。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "执行中":
            return {"ok": False, "message": "仅执行中计划可暂停"}
        if not reason.strip():
            return {"ok": False, "message": "暂停必须填写原因"}
        entry["status"] = "待执行"
        entry["上次状态"] = "已暂停"
        entry["pending"] = True
        entry["暂停原因"] = reason
        freshness_rows = store.rows("data_freshness")
        matched = [f for f in freshness_rows if f.get("监控对象") == f"window_scheduler:{entry_id}"]
        for row in matched:
            row["status"] = "延迟"
            row["时效等级"] = "延迟"
            row["pending"] = True
        return {"ok": True, "message": f"计划已暂停（{reason}），时效监控进入延迟观察"}

    def batch_trigger_due(self) -> dict[str, Any]:
        """批量触发全部到期窗口（逐个走幂等受理）。"""
        due_rows = [r for r in store.rows(self.MODULE) if r.get("status") == "待执行"]
        done, deduped, failed = 0, 0, 0
        for row in due_rows:
            result = self.trigger_window(int(row.get("id", 0)))
            if result.get("ok"):
                if result.get("idempotent"):
                    deduped += 1
                else:
                    done += 1
            else:
                failed += 1
        return {
            "ok": True,
            "message": f"批量调度完成：新受理 {done}，幂等去重 {deduped}，失败 {failed}",
            "accepted": done,
            "deduplicated": deduped,
            "failed": failed,
        }

    def consume_from_metric_catalog(self, metric_code: str) -> list[dict[str, Any]]:
        """读取指标口径中心该口径的版本信息，校验调度口径一致性。"""
        return [
            {"口径版本": m.get("口径版本", ""), "聚合窗口": m.get("聚合窗口", ""), "状态": m.get("status", "")}
            for m in store.rows("metric_catalog")
            if m.get("指标编码") == metric_code
        ]

    def rollback_window(self, entry_id: int, to_window: str) -> dict[str, Any]:
        """回退窗口重算：只拨窗口指针并登记回退来源，不动此前已受理的作业。

        回退不会删除或改写任何已有作业/回执；重放时按幂等键决定是复用原作业
        （同版次同窗口）还是产生独立可辨认的补算作业（新版次）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not to_window.strip():
            return {"ok": False, "message": "计划不存在或回退窗口为空"}
        current = str(entry.get("下次窗口", ""))
        target_key = _window_key(str(entry.get("窗口粒度", "日窗口")), to_window)
        current_key = _window_key(str(entry.get("窗口粒度", "日窗口")), current)
        # 只允许拨到当前窗口或更早窗口（禁止前进到未来）；拨到当前窗口用于切换
        # 指标版次后对同一窗口发起独立补算。
        if target_key and current_key and target_key > current_key:
            return {"ok": False, "message": "回退窗口不能晚于当前下次窗口"}
        if not target_key or target_key > current:
            return {"ok": False, "message": "回退窗口不能晚于当前下次窗口"}

        # 记录回退来源，供作业页核查；上一成功窗口的作业保持原样、不被替换。
        entry["回退来源"] = current_key or current
        entry["下次窗口"] = to_window
        entry["当前窗口键"] = target_key
        entry["status"] = "待执行"
        entry["pending"] = True
        entry["回退标记"] = to_window
        return {"ok": True, "message": f"窗口已回退到 {target_key}（来源 {entry['回退来源']}），等待重算"}

    def produce_to_anomaly_metric(self, entry_id: int) -> dict[str, Any]:
        """把连续执行失败的计划登记为指标异常事件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or str(entry.get("上次状态", "")) != "执行失败":
            return {"ok": False, "message": "计划无连续失败记录"}
        anomaly_rows = store.rows("anomaly_metric")
        anomaly_rows.append({
            "id": max((int(r.get("id", 0)) for r in anomaly_rows), default=0) + 1,
            "异常编号": f"AN-WS-{entry_id}",
            "指标编码": entry.get("指标编码", ""),
            "基线区间": entry.get("窗口粒度", ""),
            "观测值": "调度连续失败",
            "异常评分": "0.8",
            "status": "待确认",
            "pending": True,
            "abnormal": False,
        })
        return {"ok": True, "message": "调度失败已登记为异常事件"}

    def export_plan_timeline(self, metric_code: str) -> list[dict[str, Any]]:
        """导出指标的调度时间线，供指标查询服务展示刷新策略。"""
        return [
            {
                "计划名称": r.get("计划名称", ""),
                "窗口粒度": r.get("窗口粒度", ""),
                "下次窗口": r.get("下次窗口", ""),
                "上次状态": r.get("上次状态", ""),
            }
            for r in store.rows(self.MODULE) if r.get("指标编码") == metric_code
        ]

    # ---- 窗口作业回执查询 ----

    def list_receipts(self, entry_id: int, window_key: str | None = None) -> dict[str, Any]:
        """按计划（可叠加原窗口键）查回全部窗口作业回执，覆盖已受理/执行中/已确认。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "调度计划不存在或已归档", "receipts": []}
        receipts = [
            dict(r) for r in self._receipt_rows()
            if int(r.get("调度计划ID", 0)) == entry_id
            and (window_key is None or str(r.get("窗口键", "")) == window_key)
        ]
        receipts.sort(key=lambda r: str(r.get("窗口键", "")))
        return {
            "ok": True,
            "message": f"计划 {entry_id} 共 {len(receipts)} 份窗口作业回执",
            "receipts": receipts,
        }

    def get_receipt(self, receipt_no: str) -> dict[str, Any]:
        """按回执编号查回单次有效计算的受理凭证（含调度时的口径版次）。"""
        receipt = next(
            (r for r in self._receipt_rows() if r.get("回执编号") == receipt_no),
            None,
        )
        if receipt is None:
            return {"ok": False, "message": f"回执 {receipt_no} 不存在", "receipt": None}
        job = store.find("metric_compute", int(receipt.get("作业ID", 0)))
        return {"ok": True, "message": "回执已查回", "receipt": dict(receipt), "job": dict(job) if job else None}

    def window_gap_report(self, entry_id: int) -> dict[str, Any]:
        """连续日报缺口分析：列出该计划各已受理窗口与缺失窗口段。

        W1 已受理但 W0 从未完成时，报告明确标出 W0 缺段；W0 补齐后各窗口回到
        各自正确位置。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "调度计划不存在或已归档", "windows": [], "gaps": []}
        granularity = str(entry.get("窗口粒度", "日窗口"))
        receipts = [
            r for r in self._receipt_rows() if int(r.get("调度计划ID", 0)) == entry_id
        ]
        # 同窗口多版次补算时，窗口只占一个位置。
        window_state: dict[str, dict[str, Any]] = {}
        for receipt in receipts:
            key = str(receipt.get("窗口键", ""))
            job = store.find("metric_compute", int(receipt.get("作业ID", 0)))
            state = str(job.get("status", "")) if job else str(receipt.get("回执状态", ""))
            slot = window_state.setdefault(key, {
                "窗口键": key,
                "回执编号": receipt.get("回执编号", ""),
                "作业ID": receipt.get("作业ID", 0),
                "口径版本": receipt.get("口径版本", ""),
                "作业状态": state,
                "补算": bool(receipt.get("回退来源")),
            })
            # 已完成的版次作为该窗口的代表状态。
            if state == "已完成":
                slot["作业状态"] = state

        windows = sorted(window_state.values(), key=lambda item: item["窗口键"])
        gaps: list[dict[str, str]] = []
        # 锚点（计划首次受理窗口）的紧邻前序窗口若没有任何作业，说明它从未受理
        # （典型场景：W1 已受理、W0 从未完成）。只相对锚点查这一层，回退补齐 W0
        # 后该窗口已有作业，缺段自然消除，也不会沿时间轴无限向前误报。
        anchor = str(entry.get("首窗锚点", ""))
        if anchor:
            expected_before = _previous_window_key(granularity, anchor)
            before_exists = any(
                str(r.get("窗口键", "")) == expected_before for r in receipts
            ) if expected_before else False
            if expected_before and not before_exists:
                gaps.append({
                    "缺失窗口": expected_before,
                    "前一窗口": "",
                    "后一窗口": anchor,
                    "说明": f"连续日报在 {expected_before} 缺段：该窗口从未受理或未完成",
                })
        for index, slot in enumerate(windows):
            if index == 0:
                continue
            prev_key = windows[index - 1]["窗口键"]
            expected = _previous_window_key(granularity, slot["窗口键"])
            if expected and expected != prev_key:
                gaps.append({
                    "缺失窗口": expected,
                    "前一窗口": prev_key,
                    "后一窗口": slot["窗口键"],
                    "说明": f"连续日报在 {expected} 缺段：该窗口从未受理或未完成",
                })
            if slot["作业状态"] not in ("已完成",):
                # 窗口已受理但作业未完成，同样在日报中体现为未闭环段。
                gaps.append({
                    "缺失窗口": slot["窗口键"],
                    "前一窗口": prev_key,
                    "后一窗口": slot["窗口键"],
                    "说明": f"窗口 {slot['窗口键']} 作业尚未完成（当前：{slot['作业状态']}）",
                })
        return {
            "ok": True,
            "message": f"计划 {entry_id} 窗口 {len(windows)} 个，缺段 {len(gaps)} 处",
            "windows": windows,
            "gaps": gaps,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """把「立即执行」路由到幂等受理，其余动作沿用通用骨架。"""
        if action == "立即执行":
            result = self.trigger_window(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if not result.get("ok"):
                return entry, str(result.get("message", "调度失败"))
            return entry, str(result.get("message", "窗口已受理"))
        return super().run_action(entry_id, action)
