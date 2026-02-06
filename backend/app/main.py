"""
backend/app/main.py (통합 수정본)

✅ 핵심 변경점
1) metrics_window를 "30분 rollup 저장소"로 사용
2) /api/timeseries => preset 기반(12h_30m / 24h_1h / 7d_1d / 30d_1d)
3) 각 페이지 상단 KPI API 추가: /api/kpi/orders|events|alerts
4) (옵션) FastAPI 내부에서 30분마다 metrics_window_upsert_30m 실행
"""

from __future__ import annotations

import uuid
import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import asyncpg
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from pydantic import BaseModel


# -----------------------------
# 경로(절대경로로 고정)
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[2]  # fulfillment-dashboard/
load_dotenv(BASE_DIR / ".env")

WEB_DIR = BASE_DIR / "web"
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

SQL_DIR = Path(__file__).resolve().parent / "sql"
SQL_FILE = SQL_DIR / "queries.sql"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# -----------------------------
# SQL 로더: 단일 파일 파싱
# 규칙: -- name: xxx 로 시작하는 블록을 하나의 쿼리로 저장
# -----------------------------
_SQL_MAP: Dict[str, str] = {}
_SQL_LOADED = False


def load_sql_map() -> Dict[str, str]:
    global _SQL_LOADED
    if _SQL_LOADED:
        return _SQL_MAP

    text = SQL_FILE.read_text(encoding="utf-8")

    current_name = None
    buf: List[str] = []

    def flush():
        nonlocal current_name, buf
        if current_name:
            query = "\n".join(buf).strip()
            if query:
                _SQL_MAP[current_name] = query
        current_name = None
        buf = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            flush()
            current_name = stripped.split(":", 1)[1].strip()
            continue
        buf.append(line)

    flush()
    _SQL_LOADED = True
    return _SQL_MAP


def get_sql(name: str) -> str:
    sql_map = load_sql_map()
    if name not in sql_map:
        raise RuntimeError(f"SQL query not found: {name}")
    return sql_map[name]


# -----------------------------
# DB 풀
# -----------------------------
def get_database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        url = "postgresql://admin:admin@192.168.239.40:5432/fulfillment"
    return url

async def init_conn(conn: asyncpg.Connection):
        await conn.execute("SET TIME ZONE 'Asia/Seoul'")

async def create_pool_with_retry(dsn: str, min_size: int = 1, max_size: int = 10, retries: int = 30) -> asyncpg.Pool:
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size, init=init_conn)
        except Exception as e:
            last_err = e
            await asyncio.sleep(1)
    raise RuntimeError(f"DB 연결 실패(retries={retries}). 마지막 에러: {last_err}")


# -----------------------------
# metrics_window 롤업(30분) 백그라운드 태스크
# -----------------------------
KST = ZoneInfo("Asia/Seoul")


def floor_30m(dt: datetime) -> datetime:
    """
    datetime을 '30분 경계'로 내림
    예) 10:43 -> 10:30, 10:12 -> 10:00
    """
    minute = 30 if dt.minute >= 30 else 0
    return dt.replace(minute=minute, second=0, microsecond=0)


async def rollup_fill_gaps(app: FastAPI) -> None:
    """
    ✅ metrics_window를 30분 단위로 "누적" 채우는 루프
    - ENABLE_METRICS_ROLLUP=1 일 때만 실행
    - 서버가 켜져 있는 동안, 누락된 버킷을 순차적으로 upsert

    주의:
    - MVP에서는 충분하지만, 운영에선 별도 스케줄러/배치 컨테이너가 더 안정적
    """
    interval_sec = int(os.getenv("ROLLUP_CHECK_INTERVAL_SEC", "60"))  # 1분마다 확인
    bucket_min = int(os.getenv("ROLLUP_BUCKET_MIN", "30"))  # 기본 30분 (여기선 30 고정 가정)

    if bucket_min != 30:
        # 지금은 30분 전제 설계라 변경 막아둠(헷갈림 방지)
        print("[rollup] ROLLUP_BUCKET_MIN is not 30. This implementation assumes 30m buckets.")
        return

    while True:
        try:
            # 지금 시간 기준 "완료된 마지막 버킷 end" 계산
            now_kst = datetime.now(tz=KST)
            current_end = floor_30m(now_kst)  # 진행중 버킷은 제외하고, 완료된 경계까지만
            # 예: 지금 10:47이면 current_end=10:30 (10:30~11:00 버킷은 진행중)

            # DB에 마지막으로 쌓인 window_end 확인
            last = await fetch_one(get_sql("metrics_window_max_end"))
            max_end = last.get("max_end")  # timestamptz -> datetime

            # 처음이라면: 직전 30분 버킷 1개만 생성
            if not isinstance(max_end, datetime):
                window_end = current_end
                window_start = window_end - timedelta(minutes=30)
                await execute(get_sql("metrics_window_upsert_30m"), window_start, window_end)
            else:
                # 누락 버킷이 있으면 순차 생성
                # next_end = max_end + 30m 부터 current_end 까지
                next_end = max_end + timedelta(minutes=30)
                while next_end <= current_end:
                    window_end = next_end
                    window_start = window_end - timedelta(minutes=30)
                    await execute(get_sql("metrics_window_upsert_30m"), window_start, window_end)
                    next_end = next_end + timedelta(minutes=30)

        except Exception as e:
            print("[rollup] error:", repr(e))

        await asyncio.sleep(interval_sec)


_rollup_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool_with_retry(get_database_url(), min_size=1, max_size=10, retries=30)
    app.state.db_pool = pool

    # ✅ (옵션) 롤업 태스크 실행
    # - 환경변수로 켜고 끌 수 있게 함
    #   ENABLE_METRICS_ROLLUP=1 이면 켜짐
    global _rollup_task
    if os.getenv("ENABLE_METRICS_ROLLUP", "0") == "1":
        _rollup_task = asyncio.create_task(rollup_fill_gaps(app))
        print("[rollup] enabled")

    try:
        yield
    finally:
        if _rollup_task:
            _rollup_task.cancel()
        await pool.close()


app = FastAPI(title="Fulfillment Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# -----------------------------
# DB 헬퍼
# -----------------------------
async def fetch_one(sql: str, *args) -> Dict[str, Any]:
    pool: asyncpg.Pool = app.state.db_pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *args)
        return dict(row) if row else {}


async def fetch_all(sql: str, *args) -> List[Dict[str, Any]]:
    pool: asyncpg.Pool = app.state.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]


async def execute(sql: str, *args) -> str:
    pool: asyncpg.Pool = app.state.db_pool
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


# -----------------------------
# Pages
# -----------------------------
def render_page(request: Request, template_name: str, page: str, title: str, order_id: str = ""):
    return templates.TemplateResponse(
        template_name,
        {"request": request, "page": page, "page_title": title, "order_id": order_id},
    )


@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return render_page(request, "dashboard.html", "dashboard", "운영 대시보드")


@app.get("/orders", response_class=HTMLResponse)
async def page_orders(request: Request):
    return render_page(request, "orders.html", "orders", "주문")


@app.get("/orders/{order_id}", response_class=HTMLResponse)
async def page_order_detail(request: Request, order_id: str):
    return render_page(request, "order_detail.html", "order_detail", f"주문 상세: {order_id}", order_id=order_id)


@app.get("/events", response_class=HTMLResponse)
async def page_events(request: Request):
    return render_page(request, "events.html", "events", "이벤트")


@app.get("/alerts", response_class=HTMLResponse)
async def page_alerts(request: Request):
    return render_page(request, "alerts.html", "alerts", "알림(오류/홀드)")


# -----------------------------
# API Models
# -----------------------------
class AlertActionBody(BaseModel):
    note: str | None = None
    operator: str | None = "operator"


class OrderStatusUpdateBody(BaseModel):
    next_status: str
    note: str | None = None
    operator: str | None = "operator"


def parse_alert_key(alert_key: str) -> tuple[str, str]:
    # event:{event_id}  or  hold:{order_id}
    if alert_key.startswith("event:"):
        return ("EVENT", alert_key.split(":", 1)[1])
    if alert_key.startswith("hold:"):
        return ("HOLD", alert_key.split(":", 1)[1])
    raise HTTPException(status_code=400, detail="invalid alert_key (expected: event:... or hold:...)")


# -----------------------------
# APIs (Dashboard)
# -----------------------------
@app.get("/api/summary")
async def api_summary():
    row = await fetch_one(get_sql("summary"))

    def n(v, default=0):
        return default if v is None else v

    return {
        "orders_today": int(n(row.get("orders_today"), 0)),
        "shipped_today": int(n(row.get("shipped_today"), 0)),
        "backlog": int(n(row.get("backlog"), 0)),
        "holds_now": int(n(row.get("holds_now"), 0)),
        "late_shipments": int(n(row.get("late_shipments"), 0)),
        "window_start": row.get("window_start"),
        "window_end": row.get("window_end"),
        "orders_window": int(n(row.get("orders_window"), 0)),
        "payments_window": int(n(row.get("payments_window"), 0)),
        "shipped_window": int(n(row.get("shipped_window"), 0)),
        "holds_window": int(n(row.get("holds_window"), 0)),
        "hold_rate": float(n(row.get("hold_rate"), 0.0)),
        "ingest_count": int(n(row.get("ingest_count"), 0)),
        "parse_errors": int(n(row.get("parse_errors"), 0)),
        "schema_missing": int(n(row.get("schema_missing"), 0)),
        "metrics_created_at": row.get("metrics_created_at"),
    }


@app.get("/api/timeseries")
async def api_timeseries(
    preset: str = Query("24h_1h"),  # 대시보드는 기본 24h_1h가 보기 좋음
):
    """
    ✅ 프리셋 기반 시계열
    - 12h_30m / 24h_1h / 7d_1d / 30d_1d
    - metrics_window(30분 rollup) 기반
    """
    preset_map = {
        "12h_30m": "ts_12h_30m",
        "24h_1h": "ts_24h_1h",
        "7d_1d": "ts_7d_1d",
        "30d_1d": "ts_30d_1d",
    }
    if preset not in preset_map:
        raise HTTPException(status_code=400, detail="invalid preset")

    rows = await fetch_all(get_sql(preset_map[preset]))
    return {"preset": preset, "points": rows}


@app.get("/api/orders")
async def api_orders(limit: int = Query(20, ge=1, le=200)):
    rows = await fetch_all(get_sql("orders"), limit)
    return {"items": rows}


@app.get("/api/alerts")
async def api_alerts(limit: int = Query(10, ge=1, le=200)):
    rows = await fetch_all(get_sql("alerts"), limit)
    return {"items": rows}


PRESET_MAP = {
    "12h_30m": "ts_12h_30m",
    "24h_1h": "ts_24h_1h",
    "7d_1d": "ts_7d_1d",
    "30d_1d": "ts_30d_1d",
}

# -----------------------------
# APIs (상단 KPI용)
# -----------------------------
async def load_points(preset: str):
    if preset not in PRESET_MAP:
        raise HTTPException(status_code=400, detail="invalid preset")
    return await fetch_all(get_sql(PRESET_MAP[preset]))

@app.get("/api/kpi/orders")
async def api_kpi_orders(preset: str = Query("24h_1h")):
    pts = await load_points(preset)
    if not pts:
        return {"preset": preset, "updated_at": None, "orders": 0, "shipped": 0, "holds": 0,
                "backlog": 0, "holds_now": 0, "late_orders": 0, "hold_rate": 0.0}

    orders = sum(int(p.get("orders") or 0) for p in pts)
    shipped = sum(int(p.get("shipped") or 0) for p in pts)
    holds = sum(int(p.get("holds") or 0) for p in pts)

    last = pts[-1]
    backlog = int(last.get("backlog") or 0)
    holds_now = int(pts[-2].get("holds_now") or 0)
    late_orders = int(last.get("late_orders") or 0)

    hold_rate = (holds / orders) if orders > 0 else 0.0

    return {
        "preset": preset,
        "updated_at": last.get("ts"),
        "orders": orders,
        "shipped": shipped,
        "holds": holds,
        "backlog": backlog,
        "holds_now": holds_now,
        "late_orders": late_orders,
        "hold_rate": float(hold_rate),
    }


@app.get("/api/kpi/events")
async def api_kpi_events(preset: str = Query("24h_1h")):
    pts = await load_points(preset)
    if not pts:
        return {"preset": preset, "updated_at": None, "orders": 0, "shipped": 0, "holds": 0,
                "backlog": 0, "holds_now": 0, "late_orders": 0, "hold_rate": 0.0}

    total = sum(int(p.get("event_total") or 0) for p in pts)
    errors = sum(int(p.get("event_errors") or 0) for p in pts)
    error_rate = (errors / total) if total > 0 else 0.0
    
    last = pts[-1]
    delay_p95_sec = (int(p.get("delay_p95_sec") or 0) for p in pts)
    ingest_count = (int(p.get("ingest_count") or 0) for p in pts)
    toparse_errorstal = (int(p.get("parse_errors") or 0) for p in pts)
    schema_missing = (int(p.get("schema_missing") or 0) for p in pts)

    return {
        "updated_at": last.get("ts"),
        "event_total": total,
        "event_errors": errors,
        "error_rate": float(error_rate),
        "delay_p95_sec": delay_p95_sec,
        "ingest_count": ingest_count,
        "parse_errors": toparse_errorstal,
        "schema_missing": schema_missing,
    }


@app.get("/api/kpi/alerts")
async def api_kpi_alerts(preset: str = Query("24h_1h")):
    pts = await load_points(preset)
    if not pts:
        return {"preset": preset, "updated_at": None, "orders": 0, "shipped": 0, "holds": 0,
                "backlog": 0, "holds_now": 0, "late_orders": 0, "hold_rate": 0.0}

    updated_at = ((p.get("created_at") or 0) for p in pts)
    open = sum(int(p.get("alerts_open") or 0) for p in pts)
    ack = sum(int(p.get("alerts_ack") or 0) for p in pts)
    resolved = sum(int(p.get("alerts_resolved") or 0) for p in pts)
    retry = sum(int(p.get("alerts_retry") or 0) for p in pts)

    return {
        "updated_at": updated_at,
        "OPEN": open,
        "ACK": ack,
        "RESOLVED": resolved,
        "RETRY_REQUESTED": retry
    }


# -----------------------------
# APIs (Orders / Events / Alerts pages)
# -----------------------------
@app.get("/api/orders/page")
async def api_orders_page(
    status: str = Query("ALL"),
    stage: str = Query("ALL"),
    q: str = Query(""),
    limit: int = Query(200, ge=1, le=500),
):
    rows = await fetch_all(get_sql("orders_page"), status, stage, f"%{q}%", limit)
    return {"items": rows}


@app.get("/api/events/page")
async def api_events_page(
    order_id: str = Query(""),
    event_type: str = Query(""),
    only_error: bool = Query(False),
    limit: int = Query(300, ge=10, le=2000),
):
    rows = await fetch_all(get_sql("events_page"), f"%{order_id}%", f"%{event_type}%", only_error, limit)
    return {"items": rows}


@app.get("/api/alerts/page")
async def api_alerts_page(
    status: str = Query("ALL"),  # ALL/OPEN/ACK/RESOLVED/RETRY_REQUESTED
    kind: str = Query("ALL"),    # ALL/EVENT/HOLD
    limit: int = Query(300, ge=10, le=2000),
):
    rows = await fetch_all(get_sql("alerts_page"), status, kind, limit)
    return {"items": rows}


@app.get("/api/orders/{order_id}")
async def api_order_detail(
    order_id: str,
    events_limit: int = Query(200, ge=10, le=1000),
    alerts_limit: int = Query(200, ge=10, le=1000),
):
    order = await fetch_one(get_sql("order_one"), order_id)
    if not order:
        raise HTTPException(status_code=404, detail="주문을 찾을 수 없습니다.")

    events = await fetch_all(get_sql("order_events_by_order"), order_id, events_limit)
    alerts = await fetch_all(get_sql("alerts_by_order"), order_id, alerts_limit)

    return {"order": order, "events": events, "alerts": alerts}


# -----------------------------
# APIs (Ops actions)
# -----------------------------
@app.post("/api/orders/{order_id}/status")
async def api_update_order_status(order_id: str, body: OrderStatusUpdateBody):
    pool: asyncpg.Pool = app.state.db_pool

    async with pool.acquire() as conn:
        async with conn.transaction():
            before = await conn.fetchrow(get_sql("order_one"), order_id)
            if not before:
                raise HTTPException(status_code=404, detail="주문을 찾을 수 없습니다.")

            prev_status = before.get("current_status")

            # 1) orders 업데이트
            await conn.execute(get_sql("order_current_update_status"), order_id, body.next_status)

            # 2) events 로그 적재
            event_id = str(uuid.uuid4())
            await conn.execute(
                get_sql("events_insert_status_change"),
                event_id,          # $1
                order_id,          # $2
                body.next_status,  # $3 event_type  (지금 metrics에서 이 값으로 카운트하고 있어서 이게 제일 자연스러움)
                body.next_status,  # $4 current_status
                None,              # $5 reason_code (필요하면 body에 필드 추가해서 넣기)
                None,              # $6 occurred_at (None이면 now())
            )

    return {
        "ok": True,
        "order_id": order_id,
        "prev_status": prev_status,
        "status": body.next_status,
        "event_id": event_id,
    }


@app.post("/api/alerts/{alert_key}/ack")
async def api_alert_ack(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)
    kind = "EVENT"
    if kind == "EVENT":
        await execute(get_sql("events_ops_update"), target, "ACK", body.note, body.operator)
    else:  # HOLD
        await execute(get_sql("order_hold_ops_update"), target, "ACK", body.note, body.operator)

    return {"ok": True}


@app.post("/api/alerts/{alert_key}/resolve")
async def api_alert_resolve(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)
    kind = "EVENT"
    pool: asyncpg.Pool = app.state.db_pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            if kind == "EVENT":
                await conn.execute(get_sql("events_ops_update"), target, "RESOLVED", body.note, body.operator)
            else:  # HOLD
                await conn.execute(get_sql("order_hold_ops_update"), target, "RESOLVED", body.note, body.operator)
                await conn.execute(get_sql("clear_order_hold"), target)

    return {"ok": True}


@app.post("/api/alerts/{alert_key}/retry")
async def api_alert_retry(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)
    kind = "EVENT"
    note = body.note or "retry requested"
    if kind == "EVENT":
        await execute(get_sql("events_ops_update"), target, "RETRY_REQUESTED", note, body.operator)
    else:  # HOLD
        await execute(get_sql("order_hold_ops_update"), target, "RETRY_REQUESTED", note, body.operator)

    return {"ok": True}


@app.get("/health")
async def health():
    row = await fetch_one(get_sql("health"))
    return {"status": "ok", "db": bool(row.get("ok") == 1)}
