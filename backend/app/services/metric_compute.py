"""指标计算作业领域服务：管理实验指标的计算窗口、窗口水位推进与作业重试。

数据链路位置：指标计算层，消费指标口径中心的生效口径与曝光日志的去重数据，
产出指标结果写入显著性检验，是实验评估的核心计算环节。

关键不变量：
- 作业由窗口调度器受理后生成，携带 调度计划ID + 口径版本 + 窗口键 与回执编号，
  调度侧与作业侧凭同一回执编号定位同一次有效计算。
- 领取（claim）按计算批次取走已受理窗口的待执行作业，领取是幂等的：同一作业
  重复领取返回同一批次号，未完成作业不会被误标完成。
- 启动作业前核查窗口与回退来源；前序窗口缺口会阻断新窗口领取，已在途作业不受
  影响（与调度侧缺口报告口径一致）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_scheduler import _next_window_key, _previous_window_key, _window_key
from app.store import store


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MetricComputeService(BaseModuleService):
    MODULE = "metric_compute"
    ENTITY = "计算作业"
    OPERATION_TERM = "计算"
    LIST_FIELDS = [
        "作业名称", "指标编码", "口径版本", "计算窗口", "窗口键", "窗口水位",
        "回执编号", "作业类型", "回退来源", "作业状态",
    ]
    REQUIRED_FIELDS = ["作业名称", "指标编码", "计算窗口"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "计算失败"]
    # 「启动作业」由 start_job 承载带窗口核查的领域逻辑。
    ACTION_RULES = {"重试计算": "计算失败"}
    NEGATIVE_ACTIONS = []

    # ---- 内部检索 ----

    @staticmethod
    def _batch_rows() -> list[dict[str, Any]]:
        return store.rows("metric_compute_batch")

    def _receipt_for_job(self, job: dict[str, Any]) -> dict[str, Any] | None:
        receipt_no = job.get("回执编号")
        if not receipt_no:
            return None
        return next(
            (r for r in store.rows("window_dispatch_receipt") if r.get("回执编号") == receipt_no),
            None,
        )

    def _window_gap_blocks(self, job: dict[str, Any]) -> dict[str, str] | None:
        """核查前序窗口是否缺段。

        阻断口径：前序窗口完全没有任何作业（从未受理）时阻断新窗口领取；一旦
        前序窗口已受理（待执行/执行中/已完成），即视为已在补齐链路中，不再阻断，
        未闭环状态由调度侧连续日报缺口报告体现。
        """
        plan_id = int(job.get("调度计划ID", 0) or 0)
        if plan_id <= 0:
            return None
        plan = store.find("window_scheduler", plan_id)
        granularity = str(job.get("计算窗口", "日窗口"))
        target_key = str(job.get("窗口键", ""))
        # 早于锚点（计划首次受理窗口）的回退补齐窗口不再做前序缺口核查：回退
        # 目标本身就是为补齐缺口而受理，其更早窗口不在本计划连续日报范围内。
        # 锚点及之后的窗口照常核查，保证 W1 不会在 W0 缺失时被抢先领取。
        anchor = str(plan.get("首窗锚点", "")) if plan else ""
        if anchor and target_key < anchor:
            return None
        previous_key = _previous_window_key(granularity, target_key)
        if not previous_key:
            return None
        prev_job_exists = any(
            j for j in store.rows(self.MODULE)
            if int(j.get("调度计划ID", 0) or 0) == plan_id
            and str(j.get("窗口键", "")) == previous_key
        )
        if prev_job_exists:
            return None
        return {
            "missing_window": previous_key,
            "message": f"前序窗口 {previous_key} 尚未受理，窗口 {target_key} 暂不能领取（等待前序补齐）",
        }

    # ---- 业务扩展：窗口计算与结果下发 ----

    def claim_job(self, entry_id: int) -> dict[str, Any]:
        """从计算批次领取一份已受理窗口的待执行作业（幂等）。

        已领取过的作业重复领取时返回原批次号，不产生第二条批次明细，也不会
        把未完成作业标记为完成。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "计算作业不存在或已归档"}
        if entry.get("status") not in ("待执行", "执行中"):
            return {"ok": False, "message": f"作业当前状态「{entry.get('status')}」，无可领取任务"}

        # 幂等：已有批次明细直接返回。
        existing_batch = next(
            (b for b in self._batch_rows() if int(b.get("作业ID", 0)) == entry_id),
            None,
        )
        if existing_batch is not None:
            return {
                "ok": True,
                "message": f"作业 {entry_id} 已在批次 {existing_batch['批次编号']} 中，重复领取未重复建批",
                "batch": dict(existing_batch),
                "job": dict(entry),
                "idempotent": True,
            }

        # 新领取前核查前序窗口缺口。
        gap = self._window_gap_blocks(entry)
        if gap is not None:
            return {"ok": False, "message": gap["message"], "waiting_window": gap["missing_window"]}

        metric = next(
            (m for m in store.rows("metric_catalog")
             if m.get("指标编码") == entry.get("指标编码") and m.get("status") == "已生效"),
            None,
        )
        if metric is None:
            return {"ok": False, "message": f"口径 {entry.get('指标编码')} 未生效，禁止领取"}

        batch_no = f"BAT-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}"
        batch = {
            "id": max((int(r.get("id", 0)) for r in self._batch_rows()), default=0) + 1,
            "批次编号": batch_no,
            "作业ID": entry_id,
            "回执编号": entry.get("回执编号", ""),
            "调度计划ID": entry.get("调度计划ID", 0),
            "指标编码": entry.get("指标编码", ""),
            "口径版本": entry.get("口径版本", ""),
            "窗口键": entry.get("窗口键", ""),
            "计算窗口": entry.get("计算窗口", ""),
            "领取时间": _now_iso(),
            "完成时间": None,
            "批次状态": "已领取",
            "status": "已领取",
            "pending": True,
            "abnormal": False,
        }
        self._batch_rows().append(batch)
        entry["status"] = "执行中"
        entry["作业状态"] = "执行中"
        entry["pending"] = True
        entry["批次编号"] = batch_no
        return {
            "ok": True,
            "message": f"作业 {entry_id} 已领取到批次 {batch_no}",
            "batch": dict(batch),
            "job": dict(entry),
            "idempotent": False,
        }

    def claim_next_for_plan(self, plan_id: int) -> dict[str, Any]:
        """领取入口：按调度计划从计算批次取该计划下一个已受理窗口的待执行作业。"""
        plan = store.find("window_scheduler", plan_id)
        if plan is None:
            return {"ok": False, "message": f"调度计划 {plan_id} 不存在或已归档"}
        candidates = [
            j for j in store.rows(self.MODULE)
            if int(j.get("调度计划ID", 0) or 0) == plan_id and j.get("status") == "待执行"
        ]
        candidates.sort(key=lambda j: str(j.get("窗口键", "")))
        if not candidates:
            return {"ok": False, "message": f"计划 {plan_id} 没有待领取的已受理窗口作业"}
        return self.claim_job(int(candidates[0]["id"]))

    def start_job(self, entry_id: int) -> dict[str, Any]:
        """启动计算作业，推进窗口水位并在完成后写入显著性检验批次。

        作业必须先领取（存在计算批次）才能启动完成；重复启动已完成作业保持
        幂等，返回原结果而不会重复登记检验批次。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "计算作业不存在或已归档"}
        if entry.get("status") == "已完成":
            return {"ok": True, "message": f"作业 {entry_id} 已完成，重复启动未重复计算", "idempotent": True}
        if entry.get("status") != "执行中":
            return {"ok": False, "message": "仅已领取（执行中）作业可启动，请先领取"}
        metric = next(
            (m for m in store.rows("metric_catalog")
             if m.get("指标编码") == entry.get("指标编码") and m.get("status") == "已生效"),
            None,
        )
        if metric is None or metric.get("status") != "已生效":
            return {"ok": False, "message": f"口径 {entry.get('指标编码')} 未生效，禁止计算"}
        # 作业锁定调度时版次：生效口径版次与作业版次不一致时禁止串版计算。
        if entry.get("口径版本") and str(metric.get("口径版本", "")) != str(entry.get("口径版本")):
            return {
                "ok": False,
                "message": (
                    f"作业锁定版次 {entry.get('口径版本')} 与当前生效版次 "
                    f"{metric.get('口径版本')} 不一致，禁止串版计算"
                ),
            }

        batch = next(
            (b for b in self._batch_rows() if int(b.get("作业ID", 0)) == entry_id),
            None,
        )
        if batch is None:
            return {"ok": False, "message": "作业尚未领取，无计算批次，禁止直接完成"}

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["作业状态"] = "已完成"
        entry["窗口水位"] = _now_iso()
        finished_at = _now_iso()
        batch["批次状态"] = "已完成"
        batch["status"] = "已完成"
        batch["pending"] = False
        batch["完成时间"] = finished_at

        # 回执同步确认：调度侧凭回执即可回查调度时版次与确认时间。
        receipt = self._receipt_for_job(entry)
        if receipt is not None:
            receipt["回执状态"] = "已确认"
            receipt["status"] = "已确认"
            receipt["pending"] = False
            receipt["确认时间"] = finished_at

        # 调度计划闭环：该计划在该窗口之前没有其它未完成窗口时标记完成。
        plan_id = int(entry.get("调度计划ID", 0) or 0)
        if plan_id:
            plan = store.find("window_scheduler", plan_id)
            if plan is not None:
                unfinished = [
                    j for j in store.rows(self.MODULE)
                    if int(j.get("调度计划ID", 0) or 0) == plan_id
                    and j.get("status") != "已完成"
                ]
                # 推进窗口指针：已完成窗口的后一窗若晚于当前指针，则拨过去，
                # 保证后续回退/补算的窗口比较基准正确。
                granularity = str(plan.get("窗口粒度", "日窗口"))
                completed_key = str(entry.get("窗口键", ""))
                next_key = _next_window_key(granularity, completed_key)
                pointer_key = _window_key(granularity, str(plan.get("下次窗口", "")))
                if next_key and (not pointer_key or next_key > pointer_key):
                    plan["下次窗口"] = next_key
                plan["上次状态"] = "已完成"
                plan["status"] = "已完成" if not unfinished else "执行中"
                plan["pending"] = bool(unfinished)

        test_rows = store.rows("significance_test")
        test_rows.append({
            "id": max((int(r.get("id", 0)) for r in test_rows), default=0) + 1,
            "检验批次": f"ST-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}",
            "实验编码": f"EXP_-000{entry_id}",
            "指标编码": entry.get("指标编码", ""),
            "P值": "0.031",
            "置信区间": "[-0.021, 0.045]",
            "status": "待检验",
            "pending": True,
            "abnormal": False,
        })
        return {"ok": True, "message": f"作业 {entry_id} 计算完成并登记检验批次", "idempotent": False}

    def retry_failed_job(self, entry_id: int) -> dict[str, Any]:
        """重试计算失败作业，重算窗口并登记异常事件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "计算失败":
            return {"ok": False, "message": "仅计算失败作业可重试"}
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["作业状态"] = "执行中"
        entry["重试次数"] = str(int(entry.get("重试次数", "0") or 0) + 1)
        anomaly_rows = store.rows("anomaly_metric")
        anomaly_rows.append({
            "id": max((int(r.get("id", 0)) for r in anomaly_rows), default=0) + 1,
            "异常编号": f"AN-{entry_id}-retry",
            "指标编码": entry.get("指标编码", ""),
            "基线区间": entry.get("计算窗口", ""),
            "观测值": "作业重试",
            "异常评分": "0.5",
            "status": "待确认",
            "pending": True,
            "abnormal": False,
        })
        return {"ok": True, "message": f"作业 {entry_id} 已进入重试（第 {entry['重试次数']} 次）"}

    def advance_watermark(self, entry_id: int, new_watermark: str) -> dict[str, Any]:
        """推进窗口水位并同步数据时效监控的最新水位。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not new_watermark.strip():
            return {"ok": False, "message": "作业不存在或水位为空"}
        old = str(entry.get("窗口水位", ""))
        if new_watermark <= old:
            return {"ok": False, "message": "新水位必须晚于当前水位"}
        entry["窗口水位"] = new_watermark
        freshness_rows = store.rows("data_freshness")
        matched = [f for f in freshness_rows if f.get("监控对象") == f"metric_compute:{entry_id}"]
        if matched:
            matched[-1]["最新水位"] = new_watermark
            matched[-1]["status"] = "新鲜"
            matched[-1]["时效等级"] = "新鲜"
        else:
            freshness_rows.append({
                "id": max((int(r.get("id", 0)) for r in freshness_rows), default=0) + 1,
                "监控对象": f"metric_compute:{entry_id}",
                "数据域": "指标计算",
                "最新水位": new_watermark,
                "延迟分钟": "0",
                "时效等级": "新鲜",
                "status": "新鲜",
                "pending": True,
                "abnormal": False,
            })
        return {"ok": True, "message": f"水位已推进到 {new_watermark}"}

    def batch_execute_pending(self) -> dict[str, Any]:
        """批量领取并执行全部待执行作业，汇总结果。

        先领取（含窗口缺口核查）再启动，未完成作业不会被误标完成；缺口阻断的
        作业计入 blocked。
        """
        pending_rows = [r for r in store.rows(self.MODULE) if r.get("status") == "待执行"]
        done, failed, blocked = 0, 0, 0
        for row in pending_rows:
            job_id = int(row.get("id", 0))
            claim = self.claim_job(job_id)
            if not claim.get("ok"):
                if claim.get("waiting_window"):
                    blocked += 1
                else:
                    row["status"] = "计算失败"
                    row["pending"] = False
                    row["abnormal"] = True
                    failed += 1
                continue
            result = self.start_job(job_id)
            if result.get("ok"):
                done += 1
            else:
                row["status"] = "计算失败"
                row["pending"] = False
                row["abnormal"] = True
                failed += 1
        return {
            "ok": True,
            "message": f"批量执行完成：成功 {done}，缺口阻断 {blocked}，失败 {failed}",
            "success": done,
            "blocked": blocked,
            "failed": failed,
        }

    def consume_from_window_scheduler(self, metric_code: str) -> list[dict[str, Any]]:
        """读取窗口调度器中该指标的活跃计划，返回调度约束。"""
        return [
            {
                "计划名称": s.get("计划名称", ""),
                "窗口粒度": s.get("窗口粒度", ""),
                "下次窗口": s.get("下次窗口", ""),
            }
            for s in store.rows("window_scheduler")
            if s.get("指标编码") == metric_code and s.get("status") in ("待执行", "执行中")
        ]

    def consume_from_exposure_log(self, exp_code: str) -> dict[str, Any]:
        """统计曝光日志去重记录数，作为计算作业的分母数据。"""
        rows = [
            r for r in store.rows("exposure_log")
            if r.get("实验编码") == exp_code and r.get("status") == "已去重"
        ]
        return {
            "实验编码": exp_code,
            "去重曝光数": len(rows),
            "分组分布": {
                group: sum(1 for r in rows if r.get("分组名称") == group)
                for group in ("对照组", "实验组")
            },
        }

    def export_job_results(self, metric_code: str) -> list[dict[str, Any]]:
        """导出指定指标的完成作业清单，供报告生成器编排数据窗口。"""
        return [
            {
                "作业名称": r.get("作业名称", ""),
                "计算窗口": r.get("计算窗口", ""),
                "窗口水位": r.get("窗口水位", ""),
            }
            for r in store.rows(self.MODULE)
            if r.get("指标编码") == metric_code and r.get("status") == "已完成"
        ]

    # ---- 批次明细与窗口核查 ----

    def list_batches(self, plan_id: int | None = None, window_key: str | None = None) -> dict[str, Any]:
        """查询计算批次明细，可按计划/窗口过滤，与调度侧回执定位同一次计算。"""
        batches = [
            dict(b) for b in self._batch_rows()
            if (plan_id is None or int(b.get("调度计划ID", 0)) == plan_id)
            and (window_key is None or str(b.get("窗口键", "")) == window_key)
        ]
        return {"ok": True, "message": f"共 {len(batches)} 条批次明细", "batches": batches}

    def inspect_job_window(self, entry_id: int) -> dict[str, Any]:
        """作业页进入时核查窗口归属、回退来源与前序缺口。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "计算作业不存在或已归档"}
        receipt = self._receipt_for_job(entry)
        plan_id = int(entry.get("调度计划ID", 0) or 0)
        gap = self._window_gap_blocks(entry)
        plan = store.find("window_scheduler", plan_id) if plan_id else None
        return {
            "ok": True,
            "message": "窗口核查完成",
            "job": {
                "作业ID": entry_id,
                "指标编码": entry.get("指标编码", ""),
                "口径版本": entry.get("口径版本", ""),
                "窗口键": entry.get("窗口键", ""),
                "计算窗口": entry.get("计算窗口", ""),
                "回退来源": entry.get("回退来源", ""),
                "作业类型": entry.get("作业类型", "常规"),
                "作业状态": entry.get("status", ""),
                "回执编号": entry.get("回执编号", ""),
            },
            "receipt": dict(receipt) if receipt else None,
            "plan": {"计划ID": plan_id, "计划名称": plan.get("计划名称", "")} if plan else None,
            "blocked_by": gap,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """把「启动作业」路由到先领取再启动的领域流程，其余动作走通用骨架。"""
        if action == "启动作业":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if entry.get("status") == "待执行":
                claim = self.claim_job(entry_id)
                if not claim.get("ok"):
                    return entry, str(claim.get("message", "领取失败"))
            result = self.start_job(entry_id)
            return entry, str(result.get("message", "作业已处理"))
        return super().run_action(entry_id, action)
