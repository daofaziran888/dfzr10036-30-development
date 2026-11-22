"""差异样本服务：把附快照来源的外部差异样本导入为差异键回填数据集。

核心领域规则（对应外部核对样本的回填消费链路）：

1. 导入时把「源快照 + 主键 + 日志位置」绑定到每条样本，并按批次登记，
   传输重试时凭批次号幂等辨认已接纳的批次，不重复落库。
2. 同一主键出现互相冲突的样本值时，该键整键隔离（quarantine）：
   该键下全部样本行都标记隔离，原始冲突值与隔离原因逐行保留可查；
   未核实前不允许触发该键回填。
3. 事件流里的更正记录（diff_event_log）可以解除隔离：更正位点晚于
   样本位点、且更正值命中其中一个冲突值时，整键转为「已核实」，
   确认值取更正值；隔离痕迹不删除，只追加核实结论。
4. 派发按主键去重，只面向合格键（已接纳/已核实），并关联源日志起止
   位点（已核实键的截止位点覆盖更正位点），向数据比对（data_diff）
   返还受理的键值，同时按原位点生成回填任务。

状态以「主键」为单位管理；样本行（diff_sample）逐行保留原始证据。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 样本生命周期状态（键级状态，同时写到该键下的每条样本行上）
STATUS_ACCEPTED = "已接纳"        # 单值键，样本可用于回填
STATUS_QUARANTINED = "已隔离"     # 同键冲突，核实前禁止回填
STATUS_VERIFIED = "已核实"        # 冲突键被事件流更正核实
STATUS_DISPATCHED = "已派发"      # 已关联日志位点并派发给回填/比对

ELIGIBLE_STATUSES = (STATUS_ACCEPTED, STATUS_VERIFIED)


def _position_key(position: str) -> tuple[str, int]:
    """把 binlog.001234:891200 这类位点转成可比较的 (文件, 偏移)。"""
    text = str(position or "")
    if ":" in text:
        file_part, _, offset_part = text.rpartition(":")
        digits = "".join(ch for ch in offset_part if ch.isdigit())
        return file_part or text, int(digits or 0)
    digits = "".join(ch for ch in text if ch.isdigit())
    return text, int(digits or 0)


def position_after(candidate: str, baseline: str) -> bool:
    """candidate 位点是否严格晚于 baseline（同一日志文件内按偏移比较）。"""
    candidate_file, candidate_offset = _position_key(candidate)
    baseline_file, baseline_offset = _position_key(baseline)
    if candidate_file != baseline_file:
        return candidate_file > baseline_file
    return candidate_offset > baseline_offset


def later_position(left: str, right: str) -> str:
    """返回两个位点中更晚的一个。"""
    if not right:
        return left
    if not left:
        return right
    return right if position_after(right, left) else left


class DiffSampleService(BaseModuleService):
    MODULE = "diff_sample"
    ENTITY = "差异样本"
    OPERATION_TERM = "核对"
    LIST_FIELDS = [
        "批次编号", "源快照", "主键名", "主键值", "样本值",
        "日志位点", "源日志起始", "源日志截止", "隔离原因", "更正位点",
    ]
    REQUIRED_FIELDS = ["批次编号", "源快照", "主键值", "样本值", "日志位点"]
    STATUS_ORDER = [
        STATUS_ACCEPTED, STATUS_QUARANTINED, STATUS_VERIFIED, STATUS_DISPATCHED,
    ]
    # 通用动作只保留只读复核，状态流转只能走本服务的导入/更正/派发领域方法。
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """差异样本必须整批导入（绑定源快照与日志位点），禁止单条登记破坏数据集不变量。"""
        return None, ["差异样本必须通过 /api/diff-sample/imports 批次导入，不支持单条登记"]

    def list_batches(self) -> list[dict[str, Any]]:
        """列出全部样本批次的受理概况。"""
        return [
            self._batch_summary(batch)
            for batch in store.rows("diff_sample_batch")
        ]

    # ------------------------------------------------------------------
    # 批次与导入
    # ------------------------------------------------------------------
    def import_batch(
        self,
        *,
        batch_no: str,
        source_snapshot: str,
        log_start: str,
        log_end: str,
        target_table: str = "order_events_cdc",
        samples: list[dict[str, Any]] | None = None,
        content: str = "",
        content_type: str = "json",
        idempotency_key: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """接纳一个差异样本批次，返回批次汇总；批次号重复时幂等回放。"""
        batch_no = str(batch_no or "").strip()
        if not batch_no:
            return None, "批次编号不能为空"
        if not source_snapshot:
            return None, "源快照不能为空"

        batch_rows = store.rows("diff_sample_batch")
        existing = next((row for row in batch_rows if row.get("批次编号") == batch_no), None)
        if existing is not None:
            # 传输重试：辨认已经接纳的样本批次，直接回放原受理结果，不重复落库。
            return self._batch_summary(existing, replayed=True), "批次已存在，按幂等重试回放受理结果"

        parsed, parse_error = self._parse_samples(
            samples=samples, content=content, content_type=content_type)
        if parse_error:
            return None, parse_error
        if not parsed:
            return None, "批次内没有可导入的样本行"

        batch_id = max((int(row.get("id", 0)) for row in batch_rows), default=0) + 1
        batch = {
            "id": batch_id,
            "批次编号": batch_no,
            "源快照": source_snapshot,
            "源日志起始": log_start,
            "源日志截止": log_end,
            "目标表": target_table,
            "幂等键": idempotency_key or batch_no,
            "样本总数": len(parsed),
            "已接纳数": 0,
            "隔离数": 0,
            "已核实数": 0,
            "已派发数": 0,
            "status": "接纳中",
            "pending": True,
            "abnormal": False,
        }
        batch_rows.append(batch)

        # 按主键聚合识别本批次内同键冲突；同键同值视为重复佐证，不算冲突。
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in parsed:
            grouped.setdefault(str(item["主键值"]), []).append(item)

        sample_rows = store.rows(self.MODULE)
        next_id = max((int(row.get("id", 0)) for row in sample_rows), default=0) + 1
        for key_value, items in grouped.items():
            distinct_values = sorted({str(item["样本值"]) for item in items})
            conflicting = len(distinct_values) > 1
            conflict_reason = ""
            if conflicting:
                conflict_reason = (
                    f"同主键 {key_value} 在批次 {batch_no} 内存在互相冲突的样本值："
                    f"{' | '.join(distinct_values)}，核实前禁止回填"
                )
            for item in items:
                entry: dict[str, Any] = {
                    "id": next_id,
                    "批次编号": batch_no,
                    "批次ID": batch_id,
                    "源快照": source_snapshot,
                    "主键名": str(item.get("主键名") or "pk"),
                    "主键值": key_value,
                    "样本值": str(item["样本值"]),
                    "日志位点": str(item.get("日志位点") or log_start),
                    "源日志起始": log_start,
                    "源日志截止": log_end,
                    "目标表": target_table,
                    "更正位点": "",
                    "确认值": "",
                    "隔离原因": conflict_reason,
                    "原始冲突": conflict_reason,
                    "status": STATUS_QUARANTINED if conflicting else STATUS_ACCEPTED,
                    "pending": True,
                    "abnormal": conflicting,
                }
                sample_rows.append(entry)
                next_id += 1

        # 事件流里可能已经有该键的更正记录：导入时自动尝试核实刚隔离的键。
        for key_value in grouped:
            self.apply_correction(key_value)

        self._refresh_batch_counters(batch_no)
        return self._batch_summary(batch), f"批次 {batch_no} 接纳完成"

    def _parse_samples(
        self,
        *,
        samples: list[dict[str, Any]] | None,
        content: str,
        content_type: str,
    ) -> tuple[list[dict[str, Any]], str]:
        if samples:
            raw = list(samples)
        elif content.strip():
            if content_type == "csv":
                reader = csv.DictReader(io.StringIO(content.strip()))
                raw = [dict(row) for row in reader]
            else:
                try:
                    import json
                    loaded = json.loads(content)
                except ValueError as exc:
                    return [], f"样本内容不是合法 JSON：{exc}"
                if not isinstance(loaded, list):
                    return [], "JSON 样本必须是对象数组"
                raw = loaded
        else:
            return [], "未提供任何样本（samples 与 content 均为空）"

        parsed: list[dict[str, Any]] = []
        allowed = {"主键名", "主键值", "样本值", "日志位点"}
        for index, item in enumerate(raw, start=1):
            if not isinstance(item, dict):
                return [], f"第 {index} 行样本不是对象"
            if not str(item.get("主键值") or "").strip():
                return [], f"第 {index} 行样本缺少主键值"
            if not str(item.get("样本值") or "").strip():
                return [], f"第 {index} 行样本缺少样本值"
            parsed.append({key: item.get(key, "") for key in allowed})
        return parsed, ""

    def _entries_of_batch(self, batch_no: str) -> list[dict[str, Any]]:
        return [row for row in store.rows(self.MODULE) if row.get("批次编号") == batch_no]

    def _batch_summary(self, batch: dict[str, Any], *, replayed: bool = False) -> dict[str, Any]:
        entries = self._entries_of_batch(str(batch.get("批次编号")))
        # 键级视图：同主键只出一条，确认值优先取核实后的确认值。
        key_view: dict[str, dict[str, Any]] = {}
        for row in entries:
            key_value = str(row.get("主键值"))
            view = key_view.setdefault(key_value, {
                "主键值": key_value,
                "样本值": row.get("样本值"),
                "确认值": row.get("确认值", ""),
                "status": row.get("status"),
                "隔离原因": row.get("隔离原因", ""),
            })
            if row.get("确认值"):
                view["确认值"] = row.get("确认值")
                view["样本值"] = row.get("确认值")
        return {
            "批次ID": batch.get("id"),
            "批次编号": batch.get("批次编号"),
            "源快照": batch.get("源快照"),
            "源日志起始": batch.get("源日志起始"),
            "源日志截止": batch.get("源日志截止"),
            "目标表": batch.get("目标表"),
            "幂等键": batch.get("幂等键"),
            "样本总数": batch.get("样本总数"),
            "已接纳数": batch.get("已接纳数"),
            "隔离数": batch.get("隔离数"),
            "已核实数": batch.get("已核实数"),
            "已派发数": batch.get("已派发数"),
            "status": batch.get("status"),
            "replayed": replayed,
            "keys": list(key_view.values()),
        }

    # ------------------------------------------------------------------
    # 事件流更正与隔离解除（键级）
    # ------------------------------------------------------------------
    def submit_correction(
        self,
        *,
        key_value: str,
        corrected_value: str,
        position: str,
        source_snapshot: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """把事件流里的更正记录登记进 diff_event_log，并尝试解除同键隔离。"""
        key_value = str(key_value or "").strip()
        corrected_value = str(corrected_value or "").strip()
        position = str(position or "").strip()
        if not key_value or not corrected_value or not position:
            return None, "更正记录必须包含主键值、更正值和日志位点"

        log_rows = store.rows("diff_event_log")
        # 同一键同一位点重复提交视为重试，幂等处理。
        existing = next(
            (row for row in log_rows
             if row.get("主键值") == key_value and row.get("日志位点") == position),
            None,
        )
        if existing is None:
            log_rows.append({
                "id": max((int(row.get("id", 0)) for row in log_rows), default=0) + 1,
                "事件类型": "CORRECTION",
                "主键值": key_value,
                "更正值": corrected_value,
                "日志位点": position,
                "源快照": source_snapshot,
                "status": "已记录",
                "pending": False,
                "abnormal": False,
            })

        verify_result = self.apply_correction(key_value)
        verify_result["correction_position"] = position
        verify_result["corrected_value"] = corrected_value
        return verify_result, "更正已登记并完成同键核实"

    def apply_correction(self, key_value: str) -> dict[str, Any]:
        """用事件流中该键的更正记录整键核实冲突样本；隔离痕迹保留不删除。"""
        corrections = sorted(
            (row for row in store.rows("diff_event_log")
             if row.get("主键值") == key_value and row.get("事件类型") == "CORRECTION"),
            key=lambda row: _position_key(str(row.get("日志位点", ""))),
        )
        key_entries = [
            row for row in store.rows(self.MODULE)
            if row.get("主键值") == key_value and row.get("status") == STATUS_QUARANTINED
        ]
        if not key_entries:
            return {"主键值": key_value, "verified": 0, "still_quarantined": 0,
                    "verified_keys": []}

        # 该键下任一样本值被一条更晚位点的更正命中，即整键核实，确认值取更正值。
        hit = next((
            corr for corr in corrections
            if any(str(corr.get("更正值")) == str(entry.get("样本值"))
                   and position_after(str(corr.get("日志位点")), str(entry.get("日志位点")))
                   for entry in key_entries)
        ), None)
        if hit is None:
            return {"主键值": key_value, "verified": 0,
                    "still_quarantined": len({str(e.get("主键值")) for e in key_entries}),
                    "verified_keys": []}

        verified_keys = {str(hit.get("主键值"))}
        for entry in key_entries:
            entry["status"] = STATUS_VERIFIED
            entry["abnormal"] = False
            entry["pending"] = True
            entry["确认值"] = str(hit.get("更正值"))
            entry["更正位点"] = str(hit.get("日志位点"))
            # 原始冲突保留；隔离原因追加核实结论，保证全程可查。
            entry["隔离原因"] = (
                f"{entry.get('原始冲突', '')}；已由事件流位点 {hit.get('日志位点')} "
                f"的更正记录核实，确认值为 {hit.get('更正值')}"
            )
            self._refresh_batch_counters(str(entry.get("批次编号")))
        return {
            "主键值": key_value,
            "verified": len(verified_keys),
            "still_quarantined": 0,
            "verified_keys": list(verified_keys),
            "confirmed_value": str(hit.get("更正值")),
        }

    def _refresh_batch_counters(self, batch_no: str) -> None:
        batch = next(
            (row for row in store.rows("diff_sample_batch") if row.get("批次编号") == batch_no),
            None,
        )
        if batch is None:
            return
        entries = self._entries_of_batch(batch_no)
        # 计数器按主键去重，反映键级而非行数。
        keys: dict[str, set[str]] = {"已接纳": set(), "已隔离": set(),
                                     "已核实": set(), "已派发": set()}
        for row in entries:
            status = str(row.get("status"))
            if status in keys:
                keys[status].add(str(row.get("主键值")))
        batch["已接纳数"] = len(keys["已接纳"])
        batch["隔离数"] = len(keys["已隔离"])
        batch["已核实数"] = len(keys["已核实"])
        batch["已派发数"] = len(keys["已派发"])
        batch["status"] = "部分隔离" if keys["已隔离"] else (
            "已派发" if keys["已派发"] and not keys["已接纳"] and not keys["已核实"]
            else "已核实" if keys["已核实"] else "已接纳")
        batch["abnormal"] = bool(keys["已隔离"])

    # ------------------------------------------------------------------
    # 隔离视图与断点恢复（键级）
    # ------------------------------------------------------------------
    def list_quarantine(self) -> list[dict[str, Any]]:
        """返回所有曾经冲突的样本行，无论是否已核实，原始冲突与隔离原因始终可查。"""
        return [dict(row) for row in store.rows(self.MODULE) if row.get("原始冲突")]

    def _eligible_key_map(self) -> dict[str, dict[str, Any]]:
        """聚合出合格键（已接纳/已核实）的键级视图，确认值与位点按键取一条。"""
        eligible: dict[str, dict[str, Any]] = {}
        for row in store.rows(self.MODULE):
            if row.get("status") not in ELIGIBLE_STATUSES:
                continue
            key_value = str(row.get("主键值"))
            # 已核实键确认值优先；同一键多行时保留最早位点作为样本位点。
            current = eligible.get(key_value)
            confirmed = str(row.get("确认值") or row.get("样本值"))
            if current is None or (row.get("确认值") and not current.get("更正位点")):
                eligible[key_value] = {
                    "主键值": key_value,
                    "主键名": row.get("主键名"),
                    "确认值": confirmed,
                    "status": row.get("status"),
                    "批次编号": row.get("批次编号"),
                    "源快照": row.get("源快照"),
                    "日志位点": row.get("日志位点"),
                    "源日志起始": row.get("源日志起始"),
                    "源日志截止": row.get("源日志截止"),
                    "更正位点": row.get("更正位点", ""),
                    "目标表": row.get("目标表"),
                }
        return eligible

    def resume_state(self) -> dict[str, Any]:
        """断点恢复：读取差异键回填数据集，给出可恢复消费与仍被隔离的键。"""
        eligible_map = self._eligible_key_map()
        blocked_map: dict[str, dict[str, Any]] = {}
        for row in store.rows(self.MODULE):
            if row.get("status") != STATUS_QUARANTINED:
                continue
            blocked_map.setdefault(str(row.get("主键值")), {
                "主键值": row.get("主键值"),
                "批次编号": row.get("批次编号"),
                "隔离原因": row.get("隔离原因"),
                "原始冲突": row.get("原始冲突"),
            })
        return {
            "dataset": "差异键回填数据集",
            "total_samples": len(store.rows(self.MODULE)),
            "resumable_keys": list(eligible_map.values()),
            "blocked_keys": list(blocked_map.values()),
            "resumable_count": len(eligible_map),
            "blocked_count": len(blocked_map),
        }

    # ------------------------------------------------------------------
    # 派发：合格样本 + 源日志起止位点 -> 回填 + 数据比对（按主键去重）
    # ------------------------------------------------------------------
    def dispatch_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        batch = next(
            (row for row in store.rows("diff_sample_batch") if row.get("批次编号") == batch_no),
            None,
        )
        if batch is None:
            return None, f"批次 {batch_no} 不存在"

        entries = self._entries_of_batch(batch_no)
        qualified_rows = [row for row in entries if row.get("status") in ELIGIBLE_STATUSES]
        blocked_keys = {
            str(row.get("主键值")): str(row.get("隔离原因"))
            for row in entries if row.get("status") == STATUS_QUARANTINED
        }
        if not qualified_rows:
            return None, f"批次 {batch_no} 暂无合格样本，冲突键未核实前不能派发"

        # 按主键去重，一个键只派发一次；确认值取核实值或唯一样本值。
        qualified_map = self._eligible_key_map()
        qualified_map = {k: v for k, v in qualified_map.items()
                         if v["批次编号"] == batch_no}
        accepted_keys: list[dict[str, Any]] = []
        backfill_rows = store.rows("backfill_run")
        for key_value, info in qualified_map.items():
            # 已核实键的恢复截止位点必须覆盖更正事件所在位置。
            end_position = later_position(
                str(batch.get("源日志截止", "")), str(info.get("更正位点", "")))
            backfill_id = max((int(item.get("id", 0)) for item in backfill_rows), default=0) + 1
            backfill = {
                "id": backfill_id,
                "任务编号": f"BF-DIFF-{backfill_id:04d}",
                "目标表": str(info.get("目标表") or batch.get("目标表")),
                "起始位点": str(batch.get("源日志起始", "")),
                "截止位点": end_position,
                "回填模式": "按原位点差异键补数",
                "并行粒度": f"主键 {info.get('主键名')}",
                "当前进度": "0%",
                "回填速率": "待派发",
                "status": "回填中",
                "pending": True,
                "abnormal": False,
            }
            backfill_rows.append(backfill)

            # 该键下全部样本行标记已派发，保留原始冲突/核实痕迹。
            for row in entries:
                if str(row.get("主键值")) == key_value:
                    row["status"] = STATUS_DISPATCHED
                    row["pending"] = False
                    row["abnormal"] = False

            accepted_keys.append({
                "主键名": info.get("主键名"),
                "主键值": key_value,
                "受理值": info.get("确认值"),
                "源快照": info.get("源快照"),
                "日志起始": batch.get("源日志起始"),
                "日志截止": end_position,
                "回填任务": backfill["任务编号"],
            })

        # 向数据比对返还受理的键值。
        diff_rows = store.rows("data_diff")
        diff_id = max((int(row.get("id", 0)) for row in diff_rows), default=0) + 1
        diff_record = {
            "id": diff_id,
            "任务编号": f"DDF-DIFF-{diff_id:04d}",
            "源端": str(batch.get("源快照")),
            "目标端": str(batch.get("目标表")),
            "比对字段": "差异键回填受理值",
            "差异类型": "外部核对样本回填",
            "差异行数": len(accepted_keys),
            "修复建议": f"按源日志 {batch.get('源日志起始')} ~ {batch.get('源日志截止')} 原位点补数",
            "比对时间": "2026-11-22 10:00:00",
            "受理键值": accepted_keys,
            "status": "存在差异",
            "pending": True,
            "abnormal": False,
        }
        diff_rows.append(diff_record)
        self._refresh_batch_counters(batch_no)

        return {
            "批次编号": batch_no,
            "dispatched": len(accepted_keys),
            "blocked": len(blocked_keys),
            "accepted_keys": accepted_keys,
            "blocked_keys": [{"主键值": k, "隔离原因": reason}
                            for k, reason in sorted(blocked_keys.items())],
            "data_diff_id": diff_id,
        }, f"批次 {batch_no} 派发完成，受理 {len(accepted_keys)} 个键"
