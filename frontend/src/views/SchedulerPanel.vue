<template>
  <section class="domain-panel" data-testid="scheduler-panel">
    <div class="panel-head">
      <h3>窗口作业回执与连续日报</h3>
      <p class="panel-desc">
        同一计划、口径版次与计算窗口只有一份可执行作业；重复触发返回同一回执，不再分派新作业。
      </p>
    </div>

    <div class="panel-actions">
      <label class="inline-field">
        <span>选择调度计划</span>
        <select v-model.number="selectedPlanId" data-testid="plan-select" @change="onPlanChange">
          <option v-for="row in plans" :key="String(row.id)" :value="Number(row.id)">
            #{{ row.id }} {{ row['计划名称'] }}（{{ row['指标编码'] }}）
          </option>
        </select>
      </label>
      <button class="btn primary" type="button" data-testid="dispatch-btn" :disabled="!selectedPlanId" @click="onDispatch">
        立即执行（幂等受理）
      </button>
      <label class="inline-field">
        <span>回退到窗口</span>
        <input v-model="rollbackTarget" data-testid="rollback-window" placeholder="如 2026-11-24T00:00:00+00:00" />
      </label>
      <button class="btn" type="button" data-testid="rollback-btn" :disabled="!selectedPlanId" @click="onRollback">
        回退窗口
      </button>
      <button class="btn ghost" type="button" data-testid="refresh-panel" @click="reloadDomain">
        刷新回执与缺口
      </button>
    </div>

    <p v-if="domainMessage" class="domain-message" :class="{ 'is-error': domainError }" data-testid="domain-message">
      {{ domainMessage }}
    </p>

    <div class="panel-grid">
      <article class="sub-panel">
        <h4>窗口作业回执（按原窗口可查受理/执行中/已确认）</h4>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>回执编号</th>
              <th>窗口键</th>
              <th>口径版本</th>
              <th>作业ID</th>
              <th>回执状态</th>
              <th>回退来源</th>
              <th>确认时间</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="receipt in receipts" :key="receipt.回执编号" :data-testid="`receipt-${receipt.回执编号}`">
              <td>{{ receipt.回执编号 }}</td>
              <td>{{ receipt.窗口键 }}</td>
              <td>{{ receipt.口径版本 }}</td>
              <td>{{ receipt.作业ID }}</td>
              <td>{{ receipt.回执状态 }}</td>
              <td>{{ receipt.回退来源 || '—' }}</td>
              <td>{{ receipt.确认时间 ? formatTime(receipt.确认时间) : '未确认' }}</td>
            </tr>
            <tr v-if="!receipts.length">
              <td colspan="7" class="empty-state">该计划暂无窗口作业回执</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="sub-panel">
        <h4>连续日报缺口分析</h4>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>窗口键</th>
              <th>口径版本</th>
              <th>作业状态</th>
              <th>补算</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="slot in windows" :key="slot.窗口键" :data-testid="`window-slot-${slot.窗口键}`">
              <td>{{ slot.窗口键 }}</td>
              <td>{{ slot.口径版本 }}</td>
              <td>{{ slot.作业状态 }}</td>
              <td>{{ slot.补算 ? '是' : '否' }}</td>
            </tr>
            <tr v-if="!windows.length">
              <td colspan="4" class="empty-state">暂无已受理窗口</td>
            </tr>
          </tbody>
        </table>
        <ul v-if="gaps.length" class="gap-list" data-testid="gap-list">
          <li v-for="gap in gaps" :key="`${gap.缺失窗口}-${gap.说明}`" class="gap-item">
            <strong>{{ gap.缺失窗口 }}</strong>：{{ gap.说明 }}
          </li>
        </ul>
        <p v-else class="gap-ok" data-testid="gap-ok">连续日报无缺段，各窗口占正确位置</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

import {
  dispatchWindow,
  getGapReport,
  listWindowReceipts,
  rollbackWindow,
  type ReceiptRow,
  type WindowGap,
  type WindowSlot,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ plans: EntryRow[] }>()
const emit = defineEmits<{ (event: 'changed'): void }>()

const selectedPlanId = ref<number>(0)
const rollbackTarget = ref('')
const receipts = ref<ReceiptRow[]>([])
const windows = ref<WindowSlot[]>([])
const gaps = ref<WindowGap[]>([])
const domainMessage = ref('')
const domainError = ref(false)

watch(
  () => props.plans,
  (plans) => {
    if (!selectedPlanId.value && plans.length) {
      selectedPlanId.value = Number(plans[0].id)
    }
    if (selectedPlanId.value) void reloadDomain()
  },
  { immediate: true },
)

function formatTime(value: string): string {
  return value.replace('T', ' ').slice(0, 19)
}

function setMessage(message: string, isError = false) {
  domainMessage.value = message
  domainError.value = isError
}

async function onPlanChange() {
  await reloadDomain()
}

async function reloadDomain() {
  if (!selectedPlanId.value) return
  try {
    const [receiptPayload, gapPayload] = await Promise.all([
      listWindowReceipts(selectedPlanId.value),
      getGapReport(selectedPlanId.value),
    ])
    receipts.value = receiptPayload.receipts
    windows.value = gapPayload.windows
    gaps.value = gapPayload.gaps
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '回执读取失败', true)
  }
}

async function onDispatch() {
  try {
    const result = await dispatchWindow(selectedPlanId.value)
    setMessage(result.message, !result.ok)
    await reloadDomain()
    emit('changed')
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '调度受理失败', true)
  }
}

async function onRollback() {
  if (!rollbackTarget.value.trim()) {
    setMessage('请填写回退目标窗口', true)
    return
  }
  try {
    const result = await rollbackWindow(selectedPlanId.value, rollbackTarget.value.trim())
    setMessage(result.message, !result.ok)
    if (result.ok) {
      rollbackTarget.value = ''
      emit('changed')
    }
    await reloadDomain()
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '窗口回退失败', true)
  }
}

defineExpose({ reloadDomain })
</script>
