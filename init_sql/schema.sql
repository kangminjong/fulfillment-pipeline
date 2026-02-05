-- ============================================================================
-- Fulfillment Pipeline - Database Schema (NEW)
-- ============================================================================

-- ============================================================================
-- 1) 이벤트 원장 (Event Log)
-- ============================================================================
create table if not exists public.events (
  event_id        text primary key,
  order_id        text null,
  event_type      text not null,
  reason_code     text null,

  occurred_at     timestamptz not null,
  ingested_at     timestamptz not null default now(),

  source          text not null,
  payload_json    jsonb not null,

  -- 운영자/AI 개입 정보
  ops_status      text null,
  ops_note        text null,
  ops_operator    text null,
  ops_updated_at  timestamptz null
);

create index if not exists idx_events_order_time
  on public.events(order_id, occurred_at);

create index if not exists idx_events_type_time
  on public.events(event_type, occurred_at);

create index if not exists idx_events_ingested
  on public.events(ingested_at);


-- ============================================================================
-- 2) 상품 마스터 (Products)
-- ============================================================================
create table if not exists public.products (
  product_id    text primary key,
  product_name  text not null,
  stock         int not null default 0,
  updated_at    timestamptz not null default now()
);


-- ============================================================================
-- 3) 주문 현재 상태 스냅샷 (Orders)
-- ============================================================================
create table if not exists public.orders (
  order_id                 text primary key,

  -- 주문 기본 정보
  user_id                  text null,
  product_id               text null,
  product_name             text null,
  shipping_address          text null,

  -- 상태 정보
  current_stage             text not null,
  current_status            text not null,
  hold_reason_code          text null,

  last_event_type           text not null,
  last_occurred_at          timestamptz not null,

  -- 배송 정보
  tracking_no               text null,
  promised_delivery_date    date null,

  -- 운영자 HOLD 관리
  hold_ops_status           text null,
  hold_ops_note             text null,
  hold_ops_operator         text null,
  hold_ops_updated_at       timestamptz null,

  updated_at                timestamptz not null default now()
);

create index if not exists idx_orders_status
  on public.orders(current_status);

create index if not exists idx_orders_stage
  on public.orders(current_stage);

create index if not exists idx_orders_hold_reason
  on public.orders(hold_reason_code);


-- ============================================================================
-- 4) 대시보드 지표 (시간 창 기반 집계)
-- ============================================================================
create table if not exists public.metrics_window (
  window_start     timestamptz not null,
  window_end       timestamptz not null,

  orders           int not null,
  payments         int not null,
  shipped          int not null,
  holds            int not null,
  hold_rate        numeric(6,3) not null,

  ingest_count     int not null,
  parse_errors     int not null,
  schema_missing   int not null,

  created_at       timestamptz not null default now(),

  primary key (window_start, window_end)
);

create index if not exists idx_metrics_window_start
  on public.metrics_window(window_start);