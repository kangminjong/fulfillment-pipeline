"""
store.py
- "대시보드 화면이 동작하도록" 인메모리 더미 데이터 저장소
- 백그라운드에서 주문 상태가 계속 변하면서(생성→피킹→출고...) 실시간 느낌을 줌
- 나중에 Postgres/Kafka로 연결하면 이 파일의 로직을 Repository/Consumer 기반으로 교체하면 됨
"""

from __future__ import annotations

import asyncio
import random
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List

from .schemas import (
    AlertItem,
    AlertsResponse,
    OrderPriority,
    OrderRow,
    OrderStatus,
    OrderListResponse,
    SummaryResponse,
    TimePoint,
    TimeSeriesResponse,
)

UTC = timezone.utc

WAREHOUSES = ["ICN-1", "ICN-2", "PUS-1"]


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass
class _Order:
    order_id: str
    warehouse: str
    priority: OrderPriority
    status: OrderStatus
    updated_at: datetime
    eta_hours: int
    is_late: bool


class InMemoryStore:
    """
    스레드/코루틴 안전을 위해 asyncio.Lock 사용
    - FastAPI endpoint와 background loop가 동시에 접근할 수 있음
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        # 주문 데이터
        self._orders: Dict[str, _Order] = {}

        # 알림(최근 N개)
        self._alerts: Deque[AlertItem] = deque(maxlen=30)

        # 시간 시계열(최근 N개)
        self._series: Deque[TimePoint] = deque(maxlen=120)  # 대략 최근 120포인트

        # "파이프라인 상태" 느낌용 더미 지표
        self._dlq_count = 0
        self._consumer_lag = 0

        # 오늘 카운트 느낌용
        self._orders_today = 0
        self._shipped_today = 0

        # background loop 종료 플래그
        self._stop = False

        # 초기 seed
        self._seed()

    def stop(self) -> None:
        self._stop = True

    def _push_alert(self, level: str, title: str, detail: str) -> None:
        self._alerts.appendleft(
            AlertItem(
                level=level,
                title=title,
                detail=detail,
                created_at=now_utc(),
            )
        )

    def _seed(self) -> None:
        # 초기 주문 몇 개를 만들어두면 화면이 바로 풍성해짐
        for i in range(25):
            oid = f"ORD-{10000+i}"
            self._orders[oid] = _Order(
                order_id=oid,
                warehouse=random.choice(WAREHOUSES),
                priority=random.choice(list(OrderPriority)),
                status=random.choice(
                    [OrderStatus.CREATED, OrderStatus.PAID, OrderStatus.PICKING, OrderStatus.PACKED]
                ),
                updated_at=now_utc() - timedelta(minutes=random.randint(1, 120)),
                eta_hours=random.randint(1, 48),
                is_late=random.random() < 0.12,
            )

        self._push_alert("INFO", "Dashboard booted", "인메모리 데모 데이터로 대시보드가 시작됐습니다.")
        self._push_alert("WARN", "DLQ spike (demo)", "데모: DLQ 카운트가 간헐적으로 증가하도록 설정되어 있습니다.")

        # 초기 시계열도 몇 포인트 만들어두기
        base = now_utc() - timedelta(minutes=60)
        for t in range(60):
            ts = base + timedelta(minutes=t)
            self._series.append(
                TimePoint(
                    ts=ts,
                    orders_created=random.randint(0, 6),
                    shipped=random.randint(0, 5),
                    late=random.randint(0, 2),
                )
            )

    async def run_background(self) -> None:
        """
        2초마다 상태를 조금씩 업데이트.
        - 실제로는 Kafka consumer가 이벤트를 읽고 DB를 업데이트하는 역할에 해당
        """
        while not self._stop:
            async with self._lock:
                self._tick()
            await asyncio.sleep(2)

    def _tick(self) -> None:
        # 1) 신규 주문 생성(확률)
        if random.random() < 0.45:
            oid = f"ORD-{random.randint(20000, 99999)}"
            if oid not in self._orders:
                self._orders[oid] = _Order(
                    order_id=oid,
                    warehouse=random.choice(WAREHOUSES),
                    priority=random.choices(
                        population=list(OrderPriority),
                        weights=[0.25, 0.50, 0.20, 0.05],
                        k=1,
                    )[0],
                    status=OrderStatus.CREATED,
                    updated_at=now_utc(),
                    eta_hours=random.randint(6, 72),
                    is_late=False,
                )
                self._orders_today += 1

        # 2) 일부 주문 상태 진행(Created -> Paid -> Picking -> Packed -> Shipped -> Delivered)
        progression = {
            OrderStatus.CREATED: OrderStatus.PAID,
            OrderStatus.PAID: OrderStatus.PICKING,
            OrderStatus.PICKING: OrderStatus.PACKED,
            OrderStatus.PACKED: OrderStatus.SHIPPED,
            OrderStatus.SHIPPED: OrderStatus.DELIVERED,
        }

        candidates = list(self._orders.values())
        random.shuffle(candidates)

        # 한 틱에 너무 많이 바꾸면 화면이 정신없어서 적당히 제한
        for o in candidates[: random.randint(3, 7)]:
            if o.status in progression and random.random() < 0.55:
                o.status = progression[o.status]
                o.updated_at = now_utc()

                # 출고가 발생하면 shipped_today 증가(데모)
                if o.status == OrderStatus.SHIPPED:
                    self._shipped_today += 1

                # ETA 감소(데모)
                o.eta_hours = max(0, o.eta_hours - random.randint(1, 3))

                # 지연 여부(데모): urgent/high인데 오래 끌리면 late 확률을 올림
                late_bias = 0.05
                if o.priority in (OrderPriority.URGENT, OrderPriority.HIGH):
                    late_bias += 0.10
                if o.eta_hours == 0 and o.status not in (OrderStatus.DELIVERED, OrderStatus.CANCELED):
                    late_bias += 0.20

                o.is_late = random.random() < late_bias

        # 3) DLQ/lag 더미 지표 업데이트(가끔 튄다)
        self._consumer_lag = max(0, self._consumer_lag + random.randint(-3, 8))
        if random.random() < 0.18:
            self._dlq_count += random.randint(0, 3)
            if self._dlq_count > 0 and random.random() < 0.35:
                self._push_alert(
                    "ERROR",
                    "DLQ event detected",
                    "데모: 파싱 오류/스키마 불일치 같은 이벤트가 DLQ로 이동한 상황을 가정합니다.",
                )

        # 4) 시계열 포인트 추가(2초 단위지만, 대시보드에선 '분 단위처럼' 보여줘도 됨)
        self._series.append(
            TimePoint(
                ts=now_utc(),
                orders_created=random.randint(0, 4),
                shipped=random.randint(0, 4),
                late=random.randint(0, 2),
            )
        )

    async def get_summary(self) -> SummaryResponse:
        async with self._lock:
            orders = list(self._orders.values())

            backlog = sum(1 for o in orders if o.status not in (OrderStatus.DELIVERED, OrderStatus.CANCELED))
            late = sum(1 for o in orders if o.is_late and o.status not in (OrderStatus.DELIVERED, OrderStatus.CANCELED))

            shipped_like = sum(1 for o in orders if o.status in (OrderStatus.SHIPPED, OrderStatus.DELIVERED))
            # on_time_rate(데모): late가 많으면 떨어지는 식으로 단순 계산
            on_time_rate = clamp(1.0 - (late / max(1, backlog)), 0.0, 1.0)

            return SummaryResponse(
                orders_today=self._orders_today,
                shipped_today=self._shipped_today,
                backlog=backlog,
                late_shipments=late,
                on_time_rate=on_time_rate,
                dlq_count=self._dlq_count,
                consumer_lag=self._consumer_lag,
                last_updated_at=now_utc(),
            )

    async def get_orders(self, limit: int = 20) -> OrderListResponse:
        async with self._lock:
            # updated_at 기준 최신순
            rows = sorted(self._orders.values(), key=lambda o: o.updated_at, reverse=True)[:limit]
            return OrderListResponse(
                items=[
                    OrderRow(
                        order_id=o.order_id,
                        warehouse=o.warehouse,
                        priority=o.priority,
                        status=o.status,
                        is_late=o.is_late,
                        eta_hours=o.eta_hours,
                        updated_at=o.updated_at,
                    )
                    for o in rows
                ]
            )

    async def get_alerts(self) -> AlertsResponse:
        async with self._lock:
            return AlertsResponse(items=list(self._alerts))

    async def get_timeseries(self, limit: int = 60) -> TimeSeriesResponse:
        async with self._lock:
            points = list(self._series)[-limit:]
            return TimeSeriesResponse(points=points)


# 싱글톤(앱 전체에서 공유)
store = InMemoryStore()
