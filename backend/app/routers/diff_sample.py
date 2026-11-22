"""差异样本回填专用接口：导入、更正核实、隔离查询、断点恢复与派发。

通用资源路由（/api/diff_sample 列表/动作）仍然保留，本路由只承载
样本导入这类带领域规则的端点，并在资源路由之前注册以避免动态路径抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.diff_sample import DiffSampleService

router = APIRouter(prefix="/api/diff-sample", tags=["差异样本回填"])


def diff_sample_service() -> DiffSampleService:
    service = service_for("diff_sample")
    assert isinstance(service, DiffSampleService)
    return service


class SampleRow(BaseModel):
    主键名: str = "pk"
    主键值: str
    样本值: str
    日志位点: str = ""


class ImportBatchPayload(BaseModel):
    batch_no: str
    source_snapshot: str
    log_start: str
    log_end: str
    target_table: str = "order_events_cdc"
    idempotency_key: str = ""
    content_type: str = "json"
    samples: list[SampleRow] = Field(default_factory=list)
    content: str = ""


class CorrectionPayload(BaseModel):
    key_value: str
    corrected_value: str
    position: str
    source_snapshot: str = ""


def _result(ok: bool, message: str, **extra: Any) -> dict[str, Any]:
    return {"ok": ok, "message": message, **extra}


@router.post("/imports")
def import_diff_samples(payload: ImportBatchPayload) -> dict[str, Any]:
    """导入附快照来源的差异样本文件，建立差异键回填数据集。"""
    service = diff_sample_service()
    summary, message = service.import_batch(
        batch_no=payload.batch_no,
        source_snapshot=payload.source_snapshot,
        log_start=payload.log_start,
        log_end=payload.log_end,
        target_table=payload.target_table,
        samples=[row.model_dump() for row in payload.samples],
        content=payload.content,
        content_type=payload.content_type,
        idempotency_key=payload.idempotency_key,
    )
    if summary is None:
        return _result(False, message)
    return _result(True, message, batch=summary)


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    """列出全部样本批次的受理概况（含重试回放标记）。"""
    service = diff_sample_service()
    items = service.list_batches()
    return _result(True, f"共 {len(items)} 个批次", items=items, total=len(items))


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    from app.store import store

    batch = next(
        (row for row in store.rows("diff_sample_batch") if row.get("批次编号") == batch_no),
        None,
    )
    if batch is None:
        return _result(False, f"批次 {batch_no} 不存在")
    service = diff_sample_service()
    return _result(True, "批次受理记录", batch=service._batch_summary(batch))


@router.get("/quarantine")
def list_quarantine() -> dict[str, Any]:
    """隔离区：原始冲突与隔离原因（含已核实键的核实结论）始终可查。"""
    service = diff_sample_service()
    items = service.list_quarantine()
    return _result(True, f"共 {len(items)} 条冲突痕迹", items=items, total=len(items))


@router.post("/corrections")
def submit_correction(payload: CorrectionPayload) -> dict[str, Any]:
    """登记事件流更正记录并尝试解除同键隔离。"""
    service = diff_sample_service()
    result, message = service.submit_correction(
        key_value=payload.key_value,
        corrected_value=payload.corrected_value,
        position=payload.position,
        source_snapshot=payload.source_snapshot,
    )
    if result is None:
        return _result(False, message)
    return _result(True, message, result=result)


@router.get("/event-log")
def list_event_log() -> dict[str, Any]:
    """事件流更正记录，供核对核实链路与位点追踪。"""
    from app.store import store

    items = [dict(row) for row in store.rows("diff_event_log")]
    return _result(True, f"共 {len(items)} 条事件", items=items, total=len(items))


@router.get("/resume")
def resume_dataset() -> dict[str, Any]:
    """断点恢复：读取差异键回填数据集，区分可恢复消费键与仍被隔离键。"""
    service = diff_sample_service()
    state = service.resume_state()
    return _result(True, "差异键回填数据集恢复视图", **state)


@router.post("/batches/{batch_no}/dispatch")
def dispatch_batch(batch_no: str) -> dict[str, Any]:
    """派发合格样本：关联源日志起止位点，向数据比对返还受理键值并建回填任务。"""
    service = diff_sample_service()
    result, message = service.dispatch_batch(batch_no)
    if result is None:
        return _result(False, message)
    return _result(True, message, dispatch=result)
