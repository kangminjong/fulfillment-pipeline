-- =========================================================
-- queries.sql (단일 파일 로딩 방식)
-- 규칙: -- name: xxx 블록 단위
-- =========================================================


-- =========================================================
-- Health
-- =========================================================
-- name: health
SELECT 1 AS ok;


-- =========================================================
-- Dashboard Summary (수정)
-- - 미처리(backlog): 결제완료(PAID)만
-- - 완료(completed_now): DELIVERED/CANCELED
-- - 처리중(in_progress_now): PICKING/PACKED/SHIPPED
-- =========================================================
-- name: summary
WITH latest AS (
  SELECT *
  FROM metrics_window
  ORDER BY window_end DESC
  LIMIT 1
)
SELECT
  -- 오늘 생성 주문 수
  (SELECT COUNT(*) FROM orders
    WHERE created_at::date = CURRENT_DATE) AS orders_today,

  -- 오늘 shipped 로그 수 (events 로그 기반)
  (SELECT COUNT(*) FROM events
    WHERE current_status = 'SHIPPED'
      AND occurred_at::date = CURRENT_DATE) AS shipped_today,

  -- 미처리: 결제완료(PAID)만
  (SELECT COUNT(*) FROM orders
    WHERE current_status = 'PAID') AS backlog,

  -- HOLD 현재 수
  (SELECT COUNT(*) FROM orders
    WHERE current_status = 'HOLD') AS holds_now,

  -- 처리중(진행중)
  (SELECT COUNT(*) FROM orders
    WHERE current_status IN ('PICKING','PACKED','SHIPPED')) AS in_progress_now,

  -- 완료(새 KPI)
  (SELECT COUNT(*) FROM orders
    WHERE current_status IN ('DELIVERED','CANCELED')) AS completed_now,

  0 AS late_shipments,

  -- metrics_window: 없으면 0으로
  latest.window_start,
  latest.window_end,
  COALESCE(latest.orders, 0)   AS orders_window,
  COALESCE(latest.payments, 0) AS payments_window,
  COALESCE(latest.shipped, 0)  AS shipped_window,
  COALESCE(latest.holds, 0)    AS holds_window,
  COALESCE(latest.hold_rate, 0) AS hold_rate,
  COALESCE(latest.ingest_count, 0) AS ingest_count,
  COALESCE(latest.parse_errors, 0) AS parse_errors,
  COALESCE(latest.schema_missing, 0) AS schema_missing,
  to_char(
  COALESCE(latest.created_at, now()) AT TIME ZONE 'Asia/Seoul',
  'YYYY-MM-DD"T"HH24:MI:SS.MS"+09:00"'
) AS metrics_created_at
FROM (SELECT 1) d
LEFT JOIN latest ON true;


-- =========================================================
-- metrics_window (latest / max_end)
-- =========================================================
-- name: metrics_window_latest
SELECT *
FROM metrics_window
ORDER BY window_end DESC
LIMIT 1;

-- name: metrics_window_max_end
SELECT MAX(window_end) AS max_end
FROM metrics_window;


-- =========================================================
-- Timeseries presets (metrics_window 기반)
-- - preset: 12h_30m / 24h_1h / 7d_1d / 30d_1d
-- =========================================================

-- name: ts_12h_30m
WITH r AS (
  SELECT
    -- 마지막으로 "완료된" 30분 버킷 시작 시각
    (date_trunc('hour', now())
      + floor(date_part('minute', now()) / 30) * interval '30 minutes') AS end_ts,

    -- 12시간(= 30분 버킷 24개) 범위
    (date_trunc('hour', now())
      + floor(date_part('minute', now()) / 30) * interval '30 minutes'
      - interval '11 hours 30 minutes') AS start_ts
),
series AS (
  SELECT generate_series(r.start_ts, r.end_ts, interval '30 minutes') AS ts
  FROM r
),

-- 여기서 metrics_window를 ts별로 "먼저" 집계
mw_agg AS (
  SELECT
    mw.window_start AS ts,

    -- 중복 행이 있을 때 "값 자체"는 보통 동일하니 MAX로 대표값 하나만
    MAX(mw.orders)         AS orders,
    MAX(mw.payments)       AS payments,
    MAX(mw.shipped)        AS shipped,
    MAX(mw.holds)          AS holds,

    MAX(mw.ingest_count)   AS ingest_count,
    MAX(mw.parse_errors)   AS parse_errors,
    MAX(mw.schema_missing) AS schema_missing,

    MAX(mw.backlog)        AS backlog,
    MAX(mw.late_orders)    AS late_orders,

    MAX(mw.event_total)    AS event_total,
    MAX(mw.event_errors)   AS event_errors,
    MAX(mw.delay_p95_sec)  AS delay_p95_sec,

    -- 너가 원하는 부분: alerts_*는 ts별로 SUM
    SUM(mw.alerts_open)     AS alerts_open,
    SUM(mw.alerts_ack)      AS alerts_ack,
    SUM(mw.alerts_resolved) AS alerts_resolved,
    SUM(mw.alerts_retry)    AS alerts_retry
  FROM metrics_window mw
  CROSS JOIN r
  WHERE
    mw.window_start >= r.start_ts
    AND mw.window_end <= r.end_ts + interval '30 minutes'  -- 마지막 완료 버킷 포함
  GROUP BY mw.window_start
),

holds_now AS (
  SELECT COUNT(*) AS holds_now
  FROM orders
  WHERE current_status = 'HOLD'
)

SELECT
  s.ts,

  COALESCE(a.orders, 0)          AS orders,
  COALESCE(a.payments, 0)        AS payments,
  COALESCE(a.shipped, 0)         AS shipped,
  COALESCE(a.holds, 0)           AS holds,

  COALESCE(a.ingest_count, 0)    AS ingest_count,
  COALESCE(a.parse_errors, 0)    AS parse_errors,
  COALESCE(a.schema_missing, 0)  AS schema_missing,

  COALESCE(a.backlog, 0)         AS backlog,
  hn.holds_now                   AS holds_now,
  COALESCE(a.late_orders, 0)     AS late_orders,

  COALESCE(a.event_total, 0)     AS event_total,
  COALESCE(a.event_errors, 0)    AS event_errors,
  COALESCE(a.delay_p95_sec, 0)   AS delay_p95_sec,

  COALESCE(a.alerts_open, 0)     AS alerts_open,
  COALESCE(a.alerts_ack, 0)      AS alerts_ack,
  COALESCE(a.alerts_resolved, 0) AS alerts_resolved,
  COALESCE(a.alerts_retry, 0)    AS alerts_retry
FROM series s
LEFT JOIN mw_agg a ON a.ts = s.ts
CROSS JOIN holds_now hn
ORDER BY s.ts;

-- -- name: ts_12h_30m
-- WITH r AS (
--   SELECT
--     now() AS now_ts,
--     now() - interval '12 hours' AS start_ts
-- )
-- SELECT
--   mw.window_start AS ts,
--   mw.orders, mw.payments, mw.shipped, mw.holds,
--   mw.ingest_count, mw.parse_errors, mw.schema_missing,
--   mw.backlog, mw.holds_now, mw.late_orders,
--   mw.event_total, mw.event_errors, mw.delay_p95_sec,
--   mw.alerts_open, mw.alerts_ack, mw.alerts_resolved, mw.alerts_retry
-- FROM metrics_window mw, orders o
-- CROSS JOIN r
-- WHERE
--   -- 현재 시간 기준 12시간 전부터
--   mw.window_start >= r.start_ts
--   -- 현재 시간(now) 이전에 "끝난" 30분 버킷만
--   AND mw.window_end <= r.now_ts
-- ORDER BY ts;


-- name: ts_24h_1h
WITH r AS (
  SELECT
    date_trunc('hour', now()) AS end_ts,
    date_trunc('hour', now()) - interval '23 hours' AS start_ts
),
series AS (
  SELECT generate_series(r.start_ts, r.end_ts, interval '1 hour') AS ts
  FROM r
),
agg AS (
  SELECT
    date_trunc('hour', mw.window_start) AS ts,

    -- Flow 합계
    SUM(mw.orders)       AS orders,
    SUM(mw.payments)     AS payments,
    SUM(mw.shipped)      AS shipped,
    SUM(mw.holds)        AS holds,
    SUM(mw.event_total)  AS event_total,
    SUM(mw.event_errors) AS event_errors,
    SUM(mw.alerts_open)   AS alerts_open,
    SUM(mw.alerts_ack)   AS alerts_ack,
    SUM(mw.alerts_resolved)   AS alerts_resolved,
    SUM(mw.alerts_retry)   AS alerts_retry,

    -- delay: 대표값(마지막)
    (ARRAY_AGG(mw.delay_p95_sec ORDER BY mw.window_end DESC))[1] AS delay_p95_sec,

    -- Snapshot: 마지막값(있으면)
    (ARRAY_AGG(mw.backlog ORDER BY mw.window_end DESC))[1]         AS backlog,
    (SELECT COUNT(*) FROM orders WHERE current_status = 'HOLD')    AS holds_now
  FROM metrics_window mw
  CROSS JOIN r
  WHERE mw.window_start >= r.start_ts
    AND mw.window_end   <= r.end_ts + interval '1 hour'
  GROUP BY 1
)
SELECT
  s.ts,
  COALESCE(a.orders, 0)       AS orders,
  COALESCE(a.payments, 0)     AS payments,
  COALESCE(a.shipped, 0)      AS shipped,
  COALESCE(a.holds, 0)        AS holds,
  COALESCE(a.event_total, 0)  AS event_total,
  COALESCE(a.event_errors, 0) AS event_errors,
  COALESCE(a.delay_p95_sec, 0) AS delay_p95_sec,

  COALESCE(a.backlog, 0)   AS backlog,
  COALESCE(a.holds_now, 0) AS holds_now,
  COALESCE(a.alerts_open, 0) AS alerts_open,
  COALESCE(a.alerts_ack, 0) AS alerts_ack,
  COALESCE(a.alerts_resolved, 0) AS alerts_resolved,
  COALESCE(a.alerts_retry, 0) AS alerts_retry
FROM series s
LEFT JOIN agg a USING (ts)
ORDER BY s.ts;

-- name: ts_7d_1d
SELECT
  date_trunc('day', window_start) AS ts,

  SUM(orders)         AS orders,
  SUM(payments)       AS payments,
  SUM(shipped)        AS shipped,
  SUM(holds)          AS holds,
  SUM(ingest_count)   AS ingest_count,
  SUM(parse_errors)   AS parse_errors,
  SUM(schema_missing) AS schema_missing,
  SUM(event_total)    AS event_total,
  SUM(event_errors)   AS event_errors,
  SUM(alerts_open)   AS alerts_open,
  SUM(alerts_ack)   AS alerts_ack,
  SUM(alerts_resolved)   AS alerts_resolved,
  SUM(alerts_retry)   AS alerts_retry,

  (ARRAY_AGG(delay_p95_sec ORDER BY window_end DESC))[1] AS delay_p95_sec,

  (ARRAY_AGG(backlog ORDER BY window_end DESC))[1]         AS backlog,
  (SELECT COUNT(*) FROM orders WHERE current_status = 'HOLD')    AS holds_now,
  (ARRAY_AGG(late_orders ORDER BY window_end DESC))[1]     AS late_orders
FROM metrics_window
WHERE
  -- 오늘 제외: 7일 전 00:00 ~ 오늘 00:00 (딱 7일치)
  window_start >= CURRENT_DATE - INTERVAL '6 days'
  AND window_start <  CURRENT_DATE + INTERVAL '1 day'
GROUP BY 1
ORDER BY ts;

-- name: ts_30d_1d
SELECT
  date_trunc('day', window_start) AS ts,

  SUM(orders)       AS orders,
  SUM(payments)       AS payments,
  SUM(shipped)        AS shipped,
  SUM(holds)          AS holds,
  SUM(ingest_count)   AS ingest_count,
  SUM(parse_errors)   AS parse_errors,
  SUM(schema_missing) AS schema_missing,
  SUM(event_total)    AS event_total,
  SUM(event_errors)   AS event_errors,
  SUM(alerts_open)   AS alerts_open,
  SUM(alerts_ack)   AS alerts_ack,
  SUM(alerts_resolved)   AS alerts_resolved,
  SUM(alerts_retry)   AS alerts_retry,

  (ARRAY_AGG(delay_p95_sec ORDER BY window_end DESC))[1] AS delay_p95_sec,

  (ARRAY_AGG(backlog ORDER BY window_end DESC))[1]         AS backlog,
  (SELECT COUNT(*) FROM orders WHERE current_status = 'HOLD')    AS holds_now,
  (ARRAY_AGG(late_orders ORDER BY window_end DESC))[1]     AS late_orders
FROM metrics_window
WHERE
  -- 오늘 포함 30일치: (오늘-29일 00:00) ~ (내일 00:00)
  window_start >= CURRENT_DATE - INTERVAL '29 days'
  AND window_start <  CURRENT_DATE + INTERVAL '1 day'
GROUP BY 1
ORDER BY ts;


-- =========================================================
-- metrics_window upsert (30분 버킷 하나 계산해서 저장)
-- - window_start=$1, window_end=$2
-- =========================================================
-- name: metrics_window_upsert_30m
-- INSERT INTO metrics_window(
--   window_start, window_end,
--   orders, payments, shipped, holds,
--   hold_rate,
--   ingest_count, parse_errors, schema_missing,
--   backlog, holds_now, late_orders,
--   event_total, event_errors, delay_p95_sec,
--   alerts_open, alerts_ack, alerts_resolved, alerts_retry
-- )
-- VALUES (
--   $1, $2,

--   -- Flow: occurred_at 기준 (비즈 이벤트)
--   (SELECT COUNT(*) FROM events WHERE event_type='ORDER_CREATED' AND occurred_at >= $1 AND occurred_at < $2),
--   (SELECT COUNT(*) FROM events WHERE event_type='PAID'          AND occurred_at >= $1 AND occurred_at < $2),
--   (SELECT COUNT(*) FROM events WHERE event_type='SHIPPED'       AND occurred_at >= $1 AND occurred_at < $2),
--   (SELECT COUNT(*) FROM events WHERE event_type='HOLD'          AND occurred_at >= $1 AND occurred_at < $2),

--   -- hold_rate = holds/orders (0 나눗셈 방지)
--   (
--     (SELECT COUNT(*) FROM events WHERE event_type='HOLD' AND occurred_at >= $1 AND occurred_at < $2)::numeric
--     / NULLIF((SELECT COUNT(*) FROM events WHERE event_type='ORDER_CREATED' AND occurred_at >= $1 AND occurred_at < $2), 0)
--   ),

--   -- ingest_count: ingested_at 기준 (파이프라인 유입량)
--   (SELECT COUNT(*) FROM events WHERE ingested_at >= $1 AND ingested_at < $2),

--   -- parse_errors / schema_missing: 지금 테이블이 없으니 0 (확장 여지)
--   0,
--   0,

--   -- Snapshot: orders 현재 상태
--   (SELECT COUNT(*) FROM orders WHERE current_status NOT IN ('DELIVERED','CANCELED')),
--   (SELECT COUNT(*) FROM orders WHERE current_status = 'HOLD'),
--   (SELECT COUNT(*) FROM orders
--     WHERE current_status NOT IN ('DELIVERED','CANCELED')),

--   -- 이벤트 통계(윈도우 내)
--   (SELECT COUNT(*) FROM events WHERE ingested_at >= $1 AND ingested_at < $2),
--   (SELECT COUNT(*) FROM events WHERE ingested_at >= $1 AND ingested_at < $2 AND reason_code IS NOT NULL),

--   -- delay p95(초): p95(ingested - occurred)
--   COALESCE((
--     SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at)))
--     FROM events
--     WHERE ingested_at >= $1 AND ingested_at < $2
--   ), 0),

--   -- Alerts snapshot (파생: 이벤트 오류 + 주문 HOLD)
--   (
--     (SELECT COUNT(*) FROM events
--       WHERE reason_code IS NOT NULL
--         AND COALESCE(ops_status,'OPEN')='OPEN')
--     +
--     (SELECT COUNT(*) FROM orders
--       WHERE hold_reason_code IS NOT NULL
--         AND COALESCE(hold_ops_status,'OPEN')='OPEN')
--   ),
--   (
--     (SELECT COUNT(*) FROM events WHERE reason_code IS NOT NULL AND ops_status='ACK')
--     +
--     (SELECT COUNT(*) FROM orders WHERE hold_reason_code IS NOT NULL AND hold_ops_status='ACK')
--   ),
--   (
--     (SELECT COUNT(*) FROM events WHERE reason_code IS NOT NULL AND ops_status='RESOLVED')
--     +
--     (SELECT COUNT(*) FROM orders WHERE hold_reason_code IS NOT NULL AND hold_ops_status='RESOLVED')
--   ),
--   (
--     (SELECT COUNT(*) FROM events WHERE reason_code IS NOT NULL AND ops_status='RETRY_REQUESTED')
--     +
--     (SELECT COUNT(*) FROM orders WHERE hold_reason_code IS NOT NULL AND hold_ops_status='RETRY_REQUESTED')
--   )
-- )
-- ON CONFLICT (window_start, window_end)
-- DO UPDATE SET
--   orders = EXCLUDED.orders,
--   payments = EXCLUDED.payments,
--   shipped = EXCLUDED.shipped,
--   holds = EXCLUDED.holds,
--   hold_rate = EXCLUDED.hold_rate,
--   ingest_count = EXCLUDED.ingest_count,
--   parse_errors = EXCLUDED.parse_errors,
--   schema_missing = EXCLUDED.schema_missing,
--   backlog = EXCLUDED.backlog,
--   holds_now = EXCLUDED.holds_now,
--   late_orders = EXCLUDED.late_orders,
--   event_total = EXCLUDED.event_total,
--   event_errors = EXCLUDED.event_errors,
--   delay_p95_sec = EXCLUDED.delay_p95_sec,
--   alerts_open = EXCLUDED.alerts_open,
--   alerts_ack = EXCLUDED.alerts_ack,
--   alerts_resolved = EXCLUDED.alerts_resolved,
--   alerts_retry = EXCLUDED.alerts_retry,
--   created_at = now();


-- =========================================================
-- Orders (dashboard recent / page list / detail)
-- =========================================================
-- name: orders
SELECT
  order_id,
  current_stage,
  current_status,
  hold_reason_code,
  last_event_type,
  last_occurred_at,
  created_at
FROM orders
ORDER BY created_at DESC
LIMIT $1;

-- name: orders_page
SELECT
  order_id,
  current_stage,
  current_status,
  hold_reason_code,
  last_event_type,
  last_occurred_at,
  created_at
FROM orders
WHERE
  ($1 = 'ALL' OR current_status = $1)
  AND ($2 = 'ALL' OR current_stage = $2)
  AND (
    order_id ILIKE $3
  )
ORDER BY created_at DESC
LIMIT $4;

-- name: order_one
SELECT *
FROM orders
WHERE order_id = $1;


-- =========================================================
-- Events (page list / order detail)
-- =========================================================

-- name: events_page
SELECT
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  ingested_at
FROM events
WHERE
  order_id ILIKE $1
  AND event_type ILIKE $2
  AND (
    $3 = FALSE
    OR reason_code IS NOT NULL
  )
ORDER BY ingested_at DESC
LIMIT $4;

-- name: order_events_by_order
SELECT
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  ingested_at
FROM events
WHERE order_id = $1
ORDER BY ingested_at DESC
LIMIT $2;


-- =========================================================
-- Alerts (derived: EVENT error + HOLD now)
-- - key: event:{event_id} or hold:{order_id}
-- - status: coalesce(ops_status,'OPEN') / coalesce(hold_ops_status,'OPEN')
-- =========================================================

-- name: alerts
(
  SELECT
    ('event:' || e.event_id) AS alert_key,
    'EVENT' AS kind,
    COALESCE(e.ops_status, 'OPEN') AS status,
    e.order_id,
    e.event_id,
    e.reason_code AS reason,
    e.occurred_at AS occurred_at,
    e.ops_operator AS operator
  FROM events e
  WHERE e.reason_code IS NOT NULL
)
UNION ALL
(
  SELECT
    ('hold:' || o.order_id) AS alert_key,
    'HOLD' AS kind,
    COALESCE(o.hold_ops_status, 'OPEN') AS status,
    o.order_id,
    NULL AS event_id,
    o.hold_reason_code AS reason,
    o.last_occurred_at AS occurred_at,
    o.hold_ops_operator AS operator
  FROM orders o
  WHERE o.hold_reason_code IS NOT NULL
)
ORDER BY occurred_at DESC
LIMIT $1;

-- name: alerts_page
WITH base AS (
  SELECT
    ('event:' || e.event_id) AS alert_key,
    'EVENT' AS kind,
    e.order_id,
    e.event_id,
    e.reason_code AS reason,
    e.ingested_at AS occurred_at,
    e.ops_operator AS operator,
    COALESCE(e.ops_status, 'OPEN') AS status
  FROM events e
  WHERE e.reason_code IS NOT NULL
)
SELECT *
FROM base
WHERE
  ($1 = 'ALL' OR status = $1)
  AND ($2 = 'ALL' OR kind = $2)
ORDER BY occurred_at DESC
LIMIT $3;

-- name: alerts_by_order
WITH base AS (
  SELECT
    ('event:' || e.event_id) AS alert_key,
    'EVENT' AS kind,
    COALESCE(e.ops_status, 'OPEN') AS status,
    e.order_id,
    e.event_id,
    e.reason_code AS reason,
    e.occurred_at AS occurred_at,
    e.ops_operator AS operator,
    e.reason_code AS reason
  FROM events e
  WHERE e.order_id = $1
    AND e.reason_code IS NOT NULL
)
SELECT *
FROM base
ORDER BY occurred_at DESC
LIMIT $2;


-- =========================================================
-- Ops actions
-- =========================================================

-- =========================================================
-- Events insert (status change log) - 신규
-- =========================================================
-- name: events_insert_status_change
INSERT INTO events(
  event_id,
  order_id,
  event_type,
  current_status,
  reason_code,
  occurred_at,
  ingested_at
)
VALUES(
  $1,
  $2,
  $3,
  $4,
  $5,
  COALESCE($6, now()),
  now()
);

-- name: order_current_update_status
-- name: events_ops_update
WITH e_prev AS (
  SELECT
    event_id,
    ingested_at,
    COALESCE(ops_status, 'OPEN') AS prev_status
  FROM events
  WHERE event_id = $1
  FOR UPDATE
),
-- 1) 이벤트 ops 업데이트는 항상 수행
e_upd AS (
  UPDATE events
  SET
    ops_status     = $2,
    ops_note       = $3,
    ops_operator   = $4,
    ops_updated_at = now()
  WHERE event_id = $1
  RETURNING 1
),
-- 2) "상태가 실제로 바뀐 경우에만" metrics_window 증감
chg AS (
  SELECT
    ingested_at,
    prev_status,
    $2::text AS new_status
  FROM e_prev
  WHERE prev_status IS DISTINCT FROM $2::text
),
mw_upd AS (
  UPDATE metrics_window mw
  SET
    alerts_open = GREATEST(
      mw.alerts_open
        + CASE WHEN chg.prev_status = 'OPEN' THEN -1 ELSE 0 END
        + CASE WHEN chg.new_status  = 'OPEN' THEN  1 ELSE 0 END
    , 0),
    alerts_ack = GREATEST(
      mw.alerts_ack
        + CASE WHEN chg.prev_status = 'ACK' THEN -1 ELSE 0 END
        + CASE WHEN chg.new_status  = 'ACK' THEN  1 ELSE 0 END
    , 0),
    alerts_resolved = GREATEST(
      mw.alerts_resolved
        + CASE WHEN chg.prev_status = 'RESOLVED' THEN -1 ELSE 0 END
        + CASE WHEN chg.new_status  = 'RESOLVED' THEN  1 ELSE 0 END
    , 0),
    alerts_retry = GREATEST(
      mw.alerts_retry
        + CASE WHEN chg.prev_status = 'RETRY_REQUESTED' THEN -1 ELSE 0 END
        + CASE WHEN chg.new_status  = 'RETRY_REQUESTED' THEN  1 ELSE 0 END
    , 0)
  FROM chg
  -- occurred_at이 포함되는 버킷 row를 찾아 업데이트 (PK 정확히 일치 안 해도 됨)
  WHERE mw.window_start <= chg.ingested_at
    AND chg.ingested_at <  mw.window_end
  RETURNING 1
)
SELECT
  $1::text AS arg_event_id,
  $2::text AS arg_new_status,
  (SELECT COUNT(*) FROM e_prev) AS e_prev_found,
  (SELECT prev_status FROM e_prev LIMIT 1) AS prev_status,
  (SELECT COUNT(*) FROM chg) AS chg_count,
  (SELECT COUNT(*) FROM e_upd) AS event_updated,
  (SELECT COUNT(*) FROM mw_upd) AS metrics_updated;


-- UPDATE events
-- SET
--   ops_status = $2,
--   ops_note = $3,
--   ops_operator = $4,
--   ops_updated_at = now()
-- WHERE event_id = $1;

-- name: order_hold_ops_update
UPDATE orders
SET
  hold_ops_status = $2,
  hold_ops_note = $3,
  hold_ops_operator = $4,
  hold_ops_updated_at = now()
WHERE order_id = $1;

-- name: clear_order_hold
UPDATE orders
SET
  current_status = 'PAID',
  last_event_type = 'HOLD_CLEARED',
  last_occurred_at = now()
WHERE order_id = $1;
