-- 1) 이벤트
create table if not exists events (
  event_id     text primary key,
  order_id     text null,
  event_type   text not null,
  reason_code  text null,
  occurred_at  timestamptz not null,
  ingested_at  timestamptz not null default now(),
  source       text not null,
  payload_json jsonb not null
);
create index if not exists idx_events_order_time on events(order_id, occurred_at);
create index if not exists idx_events_type_time  on events(event_type, occurred_at);
create index if not exists idx_events_ingested   on events(ingested_at);

-- 2) 주문 현재 상태
create table if not exists order_current (
  order_id               text primary key,
  current_stage          text not null,
  current_status         text not null,
  hold_reason_code       text null,
  last_event_type        text not null,
  last_occurred_at       timestamptz not null,
  tracking_no            text null,
  promised_delivery_date date null,
  updated_at             timestamptz not null default now()
);
create index if not exists idx_order_current_status on order_current(current_status);
create index if not exists idx_order_current_hold   on order_current(hold_reason_code);

-- 3) 대시보드 지표(시간창)
create table if not exists metrics_window (
  window_start   timestamptz not null,
  window_end     timestamptz not null,
  orders         int not null,
  payments       int not null,
  shipped        int not null,
  holds          int not null,
  hold_rate      numeric(6,3) not null,
  ingest_count   int not null,
  parse_errors   int not null,
  schema_missing int not null,
  created_at     timestamptz not null default now(),
  primary key(window_start, window_end)
);
create index if not exists idx_metrics_window_start on metrics_window(window_start);
