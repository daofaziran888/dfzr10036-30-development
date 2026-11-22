"""业务路由汇总：差异样本专用路由先注册，通用资源路由兜底动态模块路径。"""
from __future__ import annotations

from app.routers import diff_sample, resource

ROUTERS = [diff_sample, resource]
