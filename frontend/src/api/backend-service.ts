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

// ---- 差异样本回填数据集专用入口 ----

export type DiffSampleInput = {
  主键名?: string
  主键值: string
  样本值: string
  日志位点?: string
}

export type ImportBatchInput = {
  batch_no: string
  source_snapshot: string
  log_start: string
  log_end: string
  target_table?: string
  idempotency_key?: string
  content_type?: string
  samples?: DiffSampleInput[]
  content?: string
}

async function postJson(path: string, body: Record<string, unknown>): Promise<ActionResult> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return await response.json()
}

export function importDiffSamples(payload: ImportBatchInput): Promise<ActionResult> {
  return postJson('/api/diff-sample/imports', payload as unknown as Record<string, unknown>)
}

export function submitDiffCorrection(payload: {
  key_value: string
  corrected_value: string
  position: string
  source_snapshot?: string
}): Promise<ActionResult> {
  return postJson('/api/diff-sample/corrections', payload)
}

export function dispatchDiffBatch(batchNo: string): Promise<ActionResult> {
  return postJson(`/api/diff-sample/batches/${encodeURIComponent(batchNo)}/dispatch`, {})
}

export async function fetchDiffResume(): Promise<ActionResult> {
  const response = await request('/api/diff-sample/resume')
  if (!response.ok) throw new Error('断点恢复视图读取失败')
  return await response.json()
}

export async function fetchDiffQuarantine(): Promise<ActionResult> {
  const response = await request('/api/diff-sample/quarantine')
  if (!response.ok) throw new Error('隔离区读取失败')
  return await response.json()
}
