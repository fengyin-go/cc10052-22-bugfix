"""窗口调度与指标计算的领域专用路由。

通用资源路由只承载列表/登记/通用动作；调度受理幂等、窗口作业回执、连续日报
缺口分析、作业领取与批次明细等带业务判断的能力在这里显式暴露，状态流转仍然
只发生在领域服务内。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.metric_compute import MetricComputeService
from app.services.window_scheduler import WindowSchedulerService

router = APIRouter(prefix="/api/domain", tags=["窗口调度领域"])


class RollbackPayload(BaseModel):
    to_window: str = Field(default="", alias="to_window")


class PlanIdPayload(BaseModel):
    plan_id: int | None = Field(default=None)


def _scheduler() -> WindowSchedulerService:
    service = service_for("window_scheduler")
    assert isinstance(service, WindowSchedulerService)
    return service


def _compute() -> MetricComputeService:
    service = service_for("metric_compute")
    assert isinstance(service, MetricComputeService)
    return service


# ---- 窗口调度器：受理 / 回退 / 回执 / 缺口 ----

@router.post("/window_scheduler/{entry_id}/dispatch")
def dispatch_window(entry_id: int) -> dict[str, Any]:
    """对调度窗口点立即执行：幂等受理，返回窗口作业回执。"""
    return _scheduler().trigger_window(entry_id)


@router.post("/window_scheduler/{entry_id}/rollback")
def rollback_window(entry_id: int, payload: RollbackPayload) -> dict[str, Any]:
    """回退调度窗口，只登记回退来源，不改写此前已受理作业。"""
    return _scheduler().rollback_window(entry_id, payload.to_window)


@router.get("/window_scheduler/{entry_id}/receipts")
def list_receipts(entry_id: int, window: str | None = None) -> dict[str, Any]:
    """按计划（可叠加原窗口键）查回已受理/执行中/已确认的窗口作业回执。"""
    return _scheduler().list_receipts(entry_id, window)


@router.get("/window_scheduler/receipts/{receipt_no}")
def get_receipt(receipt_no: str) -> dict[str, Any]:
    """按回执编号查回单次有效计算及其调度时口径版次。"""
    return _scheduler().get_receipt(receipt_no)


@router.get("/window_scheduler/{entry_id}/gap-report")
def gap_report(entry_id: int) -> dict[str, Any]:
    """连续日报缺口分析：标出未受理/未完成的缺段窗口。"""
    return _scheduler().window_gap_report(entry_id)


# ---- 指标计算作业：领取 / 批次 / 窗口核查 ----

@router.post("/metric_compute/{entry_id}/claim")
def claim_job(entry_id: int) -> dict[str, Any]:
    """从计算批次领取已受理窗口的作业（幂等，重复领取不重复建批）。"""
    return _compute().claim_job(entry_id)


@router.post("/metric_compute/claim-next")
def claim_next(payload: PlanIdPayload) -> dict[str, Any]:
    """领取入口：按调度计划取该计划下一个已受理窗口的待执行作业。"""
    if payload.plan_id is None:
        return {"ok": False, "message": "缺少 plan_id"}
    return _compute().claim_next_for_plan(payload.plan_id)


@router.get("/metric_compute/batches")
def list_batches(plan_id: int | None = None, window: str | None = None) -> dict[str, Any]:
    """查询计算批次明细，可按计划/窗口过滤，与调度回执定位同一次计算。"""
    return _compute().list_batches(plan_id, window)


@router.get("/metric_compute/{entry_id}/inspect")
def inspect_job(entry_id: int) -> dict[str, Any]:
    """作业页进入时核查窗口归属、回退来源与前序窗口缺口。"""
    return _compute().inspect_job_window(entry_id)
