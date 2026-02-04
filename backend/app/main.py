"""
backend/app/main.py

- Pages:
  GET /                 : dashboard.html
  GET /orders           : orders.html
  GET /orders/{order_id}: order_detail.html
  GET /events           : events.html
  GET /alerts           : alerts.html

- APIs:
  GET  /api/summary
  GET  /api/timeseries
  GET  /api/orders
  GET  /api/orders/page
  GET  /api/orders/{order_id}
  GET  /api/events/page
  GET  /api/alerts
  GET  /api/alerts/page

  POST /api/orders/{order_id}/status
  POST /api/alerts/{alert_key}/ack
  POST /api/alerts/{alert_key}/resolve
  POST /api/alerts/{alert_key}/retry
"""

from __future__ import annotations

import os
import asyncio
import uuid
from pathlib import Path
from typing import Any, Dict, List
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import asyncpg
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
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


async def create_pool_with_retry(dsn: str, min_size: int = 1, max_size: int = 10, retries: int = 30) -> asyncpg.Pool:
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)
        except Exception as e:
            last_err = e
            await asyncio.sleep(1)
    raise RuntimeError(f"DB 연결 실패(retries={retries}). 마지막 에러: {last_err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_pool_with_retry(get_database_url(), min_size=1, max_size=10, retries=30)
    app.state.db_pool = pool
    try:
        yield
    finally:
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
    next_status: str | None = None


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

    return {  # ✅ JSONResponse 제거
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
async def api_timeseries(limit: int = Query(60, ge=5, le=2000)):
    rows = await fetch_all(get_sql("timeseries"), limit)
    return {"points": rows}


@app.get("/api/orders")
async def api_orders(limit: int = Query(20, ge=1, le=200)):
    rows = await fetch_all(get_sql("orders"), limit)
    return {"items": rows}   


@app.get("/api/alerts")
async def api_alerts(limit: int = Query(10, ge=1, le=200)):
    rows = await fetch_all(get_sql("alerts"), limit)
    return ({"items": rows})


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
    return ({"items": rows})


@app.get("/api/events/page")
async def api_events_page(
    order_id: str = Query(""),
    event_type: str = Query(""),
    only_error: bool = Query(False),
    limit: int = Query(300, ge=10, le=2000),
):
    rows = await fetch_all(get_sql("events_page"), f"%{order_id}%", f"%{event_type}%", only_error, limit)
    return ({"items": rows})


@app.get("/api/alerts/page")
async def api_alerts_page(
    status: str = Query("ALL"),  # ALL/OPEN/ACK/RESOLVED/RETRY_REQUESTED
    kind: str = Query("ALL"),    # ALL/EVENT/HOLD
    limit: int = Query(300, ge=10, le=2000),
):
    rows = await fetch_all(get_sql("alerts_page"), status, kind, limit)
    return ({"items": rows})


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

    return ({"order": order, "events": events, "alerts": alerts})


# -----------------------------
# APIs (Ops actions)
# -----------------------------
@app.post("/api/orders/{order_id}/status")
async def api_update_order_status(order_id: str, body: OrderStatusUpdateBody):
    pool = app.state.db_pool
    print("dddddddddddd")
    async with pool.acquire() as conn:
        async with conn.transaction():
            before = await conn.fetchrow(get_sql("order_one"), order_id)
            prev = before["current_status"] if before else None
            print("!!!!!!!! =========>")
            # 1) orders 스냅샷 업데이트
            await conn.execute(
                get_sql("order_current_update_status"),
                order_id,
                body.next_status
            )
            print("eeeeeeeeee =========>")
            # 2) events 최신 1건 업데이트 (없으면 insert로 보강)
            reason = body.note or "manual"
            src = "dashboard"
            print("?????????? =========>", reason, "//////", src, "///////", order_id, "//////", body.next_status)
            await conn.execute(
                get_sql("event_update_latest_by_order"),
                order_id,
                body.next_status,
                reason,
                src
            )

            after = await conn.fetchrow(get_sql("order_one"), order_id)

    return {
        "ok": True,
        "order_id": order_id,
        "prev_status": prev,
        "status": after["current_status"] if after else body.next_status,
        "updated_at": after["updated_at"] if after else None,
    }




@app.post("/api/alerts/{alert_key}/ack")
async def api_alert_ack(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)
    if kind == "EVENT":
        await execute(get_sql("events_ops_update"), target, "ACK", body.note, body.operator)
    else:
        await execute(get_sql("order_hold_ops_update"), target, "ACK", body.note, body.operator)
    return ({"ok": True})


@app.post("/api/alerts/{alert_key}/resolve")
async def api_alert_resolve(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)

    if kind == "EVENT":
        await execute(get_sql("events_ops_update"), target, "RESOLVED", body.note, body.operator)
        return {"ok": True}

    # HOLD
    await execute(get_sql("order_hold_ops_update"), target, "RESOLVED", body.note, body.operator)
    await execute(get_sql("clear_order_hold"), target)

    # ✅ 선택: hold 해제와 동시에 주문 상태도 이동
    if body.next_status:
        if body.next_status == "CREATED":
            raise HTTPException(status_code=400, detail="CREATED는 수동 변경할 수 없습니다.")
        await execute(get_sql("order_current_update_status"), target, body.next_status)

    return {"ok": True, "order_id": target, "next_status": body.next_status}


@app.post("/api/alerts/{alert_key}/retry")
async def api_alert_retry(alert_key: str, body: AlertActionBody):
    kind, target = parse_alert_key(alert_key)
    note = body.note or "retry requested"
    if kind == "EVENT":
        await execute(get_sql("events_ops_update"), target, "RETRY_REQUESTED", note, body.operator)
    else:
        await execute(get_sql("order_hold_ops_update"), target, "RETRY_REQUESTED", note, body.operator)
    return ({"ok": True})


@app.get("/health")
async def health():
    row = await fetch_one(get_sql("health"))
    return {"status": "ok", "db": bool(row.get("ok") == 1)}
