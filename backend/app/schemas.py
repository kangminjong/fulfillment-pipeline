"""
schemas.py
- 프론트/백엔드가 주고받는 응답 스키마 정의(Pydantic)
- 실제 프로젝트에서는 DB 스키마/이벤트 스키마와 매핑해서 확장하면 됨
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    PAID = "PAID"
    PICKING = "PICKING"
    PACKED = "PACKED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELED = "CANCELED"
    RETURN_REQUESTED = "RETURN_REQUESTED"
    RETURNED = "RETURNED"


class OrderPriority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    URGENT = "URGENT"


class OrderRow(BaseModel):
    order_id: str
    warehouse: str
    priority: OrderPriority
    status: OrderStatus
    is_late: bool
    eta_hours: int = Field(..., description="출고/배송 예정까지 남은 시간(대략)")
    updated_at: datetime


class SummaryResponse(BaseModel):
    # KPI 카드에 보여줄 값들
    orders_today: int
    shipped_today: int
    backlog: int
    late_shipments: int
    on_time_rate: float = Field(..., description="0~1 범위")
    dlq_count: int
    consumer_lag: int
    last_updated_at: datetime


class AlertItem(BaseModel):
    level: str  # INFO/WARN/ERROR 등
    title: str
    detail: str
    created_at: datetime


class AlertsResponse(BaseModel):
    items: List[AlertItem]


class TimePoint(BaseModel):
    ts: datetime
    orders_created: int
    shipped: int
    late: int


class TimeSeriesResponse(BaseModel):
    points: List[TimePoint]


class OrderListResponse(BaseModel):
    items: List[OrderRow]
