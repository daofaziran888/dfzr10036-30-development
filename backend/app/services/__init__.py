"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.cdc_source import CdcSourceService
from app.services.binlog_parse import BinlogParseService
from app.services.event_trans import EventTransService
from app.services.dim_join import DimJoinService
from app.services.watermark_mgr import WatermarkMgrService
from app.services.out_of_order import OutOfOrderService
from app.services.sink_writer import SinkWriterService
from app.services.lag_monitor import LagMonitorService
from app.services.ddl_handle import DdlHandleService
from app.services.checkpoint import CheckpointService
from app.services.schema_evo import SchemaEvoService
from app.services.dedup_stream import DedupStreamService
from app.services.iceberg_commit import IcebergCommitService
from app.services.compaction import CompactionService
from app.services.expire_snap import ExpireSnapService
from app.services.metric_aggr import MetricAggrService
from app.services.backfill_run import BackfillRunService
from app.services.alert_rule import AlertRuleService
from app.services.data_diff import DataDiffService
from app.services.diff_sample import DiffSampleService
from app.services.task_schedule import TaskScheduleService
from app.services.stream_audit import StreamAuditService
from app.services.stream_health import StreamHealthService

SERVICES: dict[str, BaseModuleService] = {
    "cdc_source": CdcSourceService(),
    "binlog_parse": BinlogParseService(),
    "event_trans": EventTransService(),
    "dim_join": DimJoinService(),
    "watermark_mgr": WatermarkMgrService(),
    "out_of_order": OutOfOrderService(),
    "sink_writer": SinkWriterService(),
    "lag_monitor": LagMonitorService(),
    "ddl_handle": DdlHandleService(),
    "checkpoint": CheckpointService(),
    "schema_evo": SchemaEvoService(),
    "dedup_stream": DedupStreamService(),
    "iceberg_commit": IcebergCommitService(),
    "compaction": CompactionService(),
    "expire_snap": ExpireSnapService(),
    "metric_aggr": MetricAggrService(),
    "backfill_run": BackfillRunService(),
    "alert_rule": AlertRuleService(),
    "data_diff": DataDiffService(),
    "diff_sample": DiffSampleService(),
    "task_schedule": TaskScheduleService(),
    "stream_audit": StreamAuditService(),
    "stream_health": StreamHealthService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)