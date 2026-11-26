<template>
  <section class="domain-panel" data-testid="compute-panel">
    <div class="panel-head">
      <h3>作业领取、批次明细与窗口核查</h3>
      <p class="panel-desc">
        领取从计算批次取走已受理窗口的作业，重复领取返回同一批次号；调度侧回执与作业侧批次
        凭同一编号定位同一次有效计算。
      </p>
    </div>

    <div class="panel-actions">
      <label class="inline-field">
        <span>按调度计划领取</span>
        <select v-model.number="claimPlanId" data-testid="claim-plan-select">
          <option v-for="row in plans" :key="String(row.id)" :value="Number(row.id)">
            #{{ row.id }} {{ row['计划名称'] }}
          </option>
        </select>
      </label>
      <button class="btn primary" type="button" data-testid="claim-next-btn" :disabled="!claimPlanId" @click="onClaimNext">
        领取下一个窗口作业
      </button>
      <button class="btn ghost" type="button" data-testid="refresh-batches" @click="reloadBatches">
        刷新批次明细
      </button>
    </div>

    <p v-if="domainMessage" class="domain-message" :class="{ 'is-error': domainError }" data-testid="compute-message">
      {{ domainMessage }}
    </p>

    <div class="panel-grid">
      <article class="sub-panel">
        <h4>计算批次明细（与调度回执同一编号）</h4>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>批次编号</th>
              <th>回执编号</th>
              <th>作业ID</th>
              <th>窗口键</th>
              <th>口径版本</th>
              <th>批次状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="batch in batches" :key="batch.批次编号" :data-testid="`batch-${batch.批次编号}`">
              <td>{{ batch.批次编号 }}</td>
              <td>{{ batch.回执编号 }}</td>
              <td>{{ batch.作业ID }}</td>
              <td>{{ batch.窗口键 }}</td>
              <td>{{ batch.口径版本 }}</td>
              <td>{{ batch.批次状态 }}</td>
            </tr>
            <tr v-if="!batches.length">
              <td colspan="6" class="empty-state">暂无计算批次，先在调度器受理窗口并领取</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="sub-panel">
        <h4>作业窗口核查（进入作业时校验窗口与回退来源）</h4>
        <label class="inline-field">
          <span>选择作业</span>
          <select v-model.number="inspectJobId" data-testid="inspect-job-select" @change="onInspect">
            <option v-for="row in jobs" :key="String(row.id)" :value="Number(row.id)">
              #{{ row.id }} {{ row['作业名称'] }}（{{ row['窗口键'] ?? '—' }}）
            </option>
          </select>
        </label>
        <button class="btn" type="button" data-testid="inspect-btn" :disabled="!inspectJobId" @click="onInspect">
          核查窗口
        </button>
        <button
          class="btn primary"
          type="button"
          data-testid="claim-one-btn"
          :disabled="!inspectJobId"
          @click="onClaimOne"
        >
          领取该作业（幂等）
        </button>
        <dl v-if="inspection" class="inspect-grid" data-testid="inspect-result">
          <dt>回执编号</dt><dd>{{ inspection.job.回执编号 || '—' }}</dd>
          <dt>窗口键</dt><dd>{{ inspection.job.窗口键 }}</dd>
          <dt>口径版本</dt><dd>{{ inspection.job.口径版本 }}</dd>
          <dt>作业类型</dt><dd>{{ inspection.job.作业类型 }}</dd>
          <dt>回退来源</dt><dd>{{ inspection.job.回退来源 || '非回退作业' }}</dd>
          <dt>作业状态</dt><dd>{{ inspection.job.作业状态 }}</dd>
          <dt>前序缺口</dt>
          <dd :class="{ 'gap-warn': inspection.blocked_by }">
            {{ inspection.blocked_by ? inspection.blocked_by.message : '无阻断' }}
          </dd>
        </dl>
        <p v-else class="empty-state">选择作业后核查其窗口归属与回退来源</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

import {
  claimComputeJob,
  claimNextForPlan,
  inspectComputeJob,
  listComputeBatches,
  type BatchRow,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

type Inspection = {
  job: Record<string, string>
  receipt: Record<string, unknown> | null
  plan: Record<string, unknown> | null
  blocked_by: { missing_window: string; message: string } | null
}

const props = defineProps<{ jobs: EntryRow[]; plans: EntryRow[] }>()
const emit = defineEmits<{ (event: 'changed'): void }>()

const claimPlanId = ref<number>(0)
const inspectJobId = ref<number>(0)
const batches = ref<BatchRow[]>([])
const inspection = ref<Inspection | null>(null)
const domainMessage = ref('')
const domainError = ref(false)

watch(
  () => props.plans,
  (plans) => {
    if (!claimPlanId.value && plans.length) claimPlanId.value = Number(plans[0].id)
  },
  { immediate: true },
)
watch(
  () => props.jobs,
  (jobs) => {
    if (!inspectJobId.value && jobs.length) inspectJobId.value = Number(jobs[0].id)
  },
  { immediate: true },
)

function setMessage(message: string, isError = false) {
  domainMessage.value = message
  domainError.value = isError
}

async function reloadBatches() {
  try {
    const payload = await listComputeBatches()
    batches.value = payload.batches
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '批次明细读取失败', true)
  }
}

async function onClaimNext() {
  try {
    const result = await claimNextForPlan(claimPlanId.value)
    setMessage(result.message, !result.ok)
    if (result.ok) {
      await reloadBatches()
      emit('changed')
    }
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '领取失败', true)
  }
}

async function onInspect() {
  if (!inspectJobId.value) return
  try {
    const payload = await inspectComputeJob(inspectJobId.value)
    if (!payload.ok) {
      setMessage(payload.message, true)
      return
    }
    inspection.value = payload as unknown as Inspection
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '窗口核查失败', true)
  }
}

async function onClaimOne() {
  if (!inspectJobId.value) return
  try {
    const claim = await claimComputeJob(inspectJobId.value)
    setMessage(claim.message, !claim.ok)
    if (claim.ok) {
      await reloadBatches()
      emit('changed')
    }
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '领取失败', true)
  }
}

defineExpose({ reloadBatches })
</script>
