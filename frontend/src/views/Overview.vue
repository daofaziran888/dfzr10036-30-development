<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "CDC数据源", "created": 0, "pending": 0, "abnormal": 0}, {"name": "日志解析", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件转换", "created": 0, "pending": 0, "abnormal": 0}, {"name": "维表关联", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "乱序处理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "结果落盘", "created": 0, "pending": 0, "abnormal": 0}, {"name": "延迟监控", "created": 0, "pending": 0, "abnormal": 0}, {"name": "DDL处理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "断点恢复", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema演进", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流去重", "created": 0, "pending": 0, "abnormal": 0}, {"name": "湖仓提交", "created": 0, "pending": 0, "abnormal": 0}, {"name": "小文件合并", "created": 0, "pending": 0, "abnormal": 0}, {"name": "快照过期", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流聚合", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量回填", "created": 0, "pending": 0, "abnormal": 0}, {"name": "告警规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据比对", "created": 0, "pending": 0, "abnormal": 0}, {"name": "差异样本回填", "created": 0, "pending": 0, "abnormal": 0}, {"name": "编排调度", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流审计", "created": 0, "pending": 0, "abnormal": 0}, {"name": "健康报告", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
