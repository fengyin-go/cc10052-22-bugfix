import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 窗口调度 / 指标计算领域接口 ----

async function postDomain<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(`/api/domain${path}`, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  return (await response.json()) as T
}

async function getDomain<T>(path: string, query: Record<string, string | number> = {}): Promise<T> {
  const params = new URLSearchParams(
    Object.entries(query).map(([key, value]) => [key, String(value)]),
  ).toString()
  const suffix = params ? `?${params}` : ''
  const response = await request(`/api/domain${path}${suffix}`)
  if (!response.ok) throw new Error('领域数据读取失败')
  return (await response.json()) as T
}

export type DomainResult<T = unknown> = {
  ok: boolean
  message: string
  idempotent?: boolean
  waiting_window?: string
} & T

/** 对调度窗口点立即执行：幂等受理，返回窗口作业回执。 */
export function dispatchWindow(planId: number): Promise<DomainResult<{ receipt?: ReceiptRow; job?: EntryRow }>> {
  return postDomain(`/window_scheduler/${planId}/dispatch`)
}

/** 回退调度窗口，只登记回退来源，不改写此前已受理作业。 */
export function rollbackWindow(
  planId: number,
  toWindow: string,
): Promise<DomainResult> {
  return postDomain(`/window_scheduler/${planId}/rollback`, { to_window: toWindow })
}

/** 按计划（可叠加原窗口键）查回窗口作业回执。 */
export function listWindowReceipts(
  planId: number,
  window?: string,
): Promise<DomainResult<{ receipts: ReceiptRow[] }>> {
  return getDomain(`/window_scheduler/${planId}/receipts`, window ? { window } : {})
}

/** 按回执编号查回单次有效计算及其调度时口径版次。 */
export function getWindowReceipt(
  receiptNo: string,
): Promise<DomainResult<{ receipt: ReceiptRow | null; job?: EntryRow | null }>> {
  return getDomain(`/window_scheduler/receipts/${encodeURIComponent(receiptNo)}`)
}

/** 连续日报缺口分析。 */
export function getGapReport(
  planId: number,
): Promise<DomainResult<{ windows: WindowSlot[]; gaps: WindowGap[] }>> {
  return getDomain(`/window_scheduler/${planId}/gap-report`)
}

/** 从计算批次领取已受理窗口的作业（幂等）。 */
export function claimComputeJob(
  jobId: number,
): Promise<DomainResult<{ batch?: BatchRow; job?: EntryRow }>> {
  return postDomain(`/metric_compute/${jobId}/claim`)
}

/** 领取入口：按调度计划取下一个已受理窗口的待执行作业。 */
export function claimNextForPlan(
  planId: number,
): Promise<DomainResult<{ batch?: BatchRow; job?: EntryRow }>> {
  return postDomain('/metric_compute/claim-next', { plan_id: planId })
}

/** 查询计算批次明细，可按计划/窗口过滤。 */
export function listComputeBatches(
  planId?: number,
  window?: string,
): Promise<DomainResult<{ batches: BatchRow[] }>> {
  const query: Record<string, string | number> = {}
  if (planId !== undefined) query.plan_id = planId
  if (window) query.window = window
  return getDomain('/metric_compute/batches', query)
}

/** 作业页进入时核查窗口归属、回退来源与前序缺口。 */
export function inspectComputeJob(
  jobId: number,
): Promise<DomainResult<Record<string, unknown>>> {
  return getDomain(`/metric_compute/${jobId}/inspect`)
}

export type ReceiptRow = {
  id: number
  回执编号: string
  调度计划ID: number
  计划名称: string
  指标编码: string
  口径版本: string
  计算窗口: string
  窗口键: string
  窗口水位: string
  作业ID: number
  受理时间: string | null
  确认时间: string | null
  回退来源: string
  回执状态: string
}

export type BatchRow = {
  id: number
  批次编号: string
  作业ID: number
  回执编号: string
  调度计划ID: number
  指标编码: string
  口径版本: string
  窗口键: string
  计算窗口: string
  领取时间: string | null
  完成时间: string | null
  批次状态: string
}

export type WindowSlot = {
  窗口键: string
  回执编号: string
  作业ID: number
  口径版本: string
  作业状态: string
  补算: boolean
}

export type WindowGap = {
  缺失窗口: string
  前一窗口: string
  后一窗口: string
  说明: string
}
