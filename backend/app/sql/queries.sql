-- =========================================
-- queries.sql (named queries)
-- =========================================

-- name: health
SELECT 1 AS ok;


-- name: summary
WITH mw AS (
  SELECT *
  FROM metrics_window
  ORDER BY window_end DESC
  LIMIT 1
),
oc AS (
  SELECT
    COUNT(*) FILTER (WHERE current_status NOT IN ('DELIVERED','CANCELED')) AS backlog,
    COUNT(*) FILTER (WHERE hold_reason_code IS NOT NULL AND hold_reason_code <> '') AS holds_now,
    COUNT(*) FILTER (
      WHERE promised_delivery_date IS NOT NULL
        AND promised_delivery_date < CURRENT_DATE
        AND current_status NOT IN ('DELIVERED','CANCELED')
    ) AS late_shipments
  FROM orders
),
today AS (
  SELECT
    COUNT(*) FILTER (WHERE event_type = 'ORDER_CREATED') AS orders_today,
    COUNT(*) FILTER (WHERE event_type = 'SHIPPED') AS shipped_today
  FROM events
  WHERE occurred_at >= date_trunc('day', now())
)
SELECT
  today.orders_today,
  today.shipped_today,
  oc.backlog,
  oc.holds_now,
  oc.late_shipments,

  mw.window_start,
  mw.window_end,
  mw.orders   AS orders_window,
  mw.payments AS payments_window,
  mw.shipped  AS shipped_window,
  mw.holds    AS holds_window,
  mw.hold_rate,
  mw.ingest_count,
  mw.parse_errors,
  mw.schema_missing,
  mw.created_at AS metrics_created_at
FROM mw, oc, today;


-- name: timeseries
SELECT
  date_trunc('hour', window_end) AS ts,
  SUM(orders)  AS orders_created,
  SUM(shipped) AS shipped,
  SUM(holds)   AS holds
FROM metrics_window
GROUP BY 1
ORDER BY ts DESC
LIMIT $1;


-- name: orders
SELECT
  o.order_id,
  o.product_id,
  o.product_name,
  o.current_stage,
  o.current_status,
  o.hold_reason_code,
  o.last_event_type,
  o.last_occurred_at,
  o.tracking_no,
  o.promised_delivery_date,
  o.updated_at,
  o.shipping_address,
  o.user_id,

  CASE
    WHEN o.promised_delivery_date IS NULL THEN false
    WHEN o.promised_delivery_date < CURRENT_DATE AND o.current_status NOT IN ('DELIVERED','CANCELED') THEN true
    ELSE false
  END AS is_late
FROM orders o
ORDER BY o.updated_at DESC
LIMIT $1;


-- name: orders_page
SELECT
  o.order_id,
  o.product_id,
  o.product_name,
  o.current_stage,
  o.current_status,
  o.hold_reason_code,
  o.last_event_type,
  o.last_occurred_at,
  o.tracking_no,
  o.promised_delivery_date,
  o.updated_at,
  o.shipping_address,
  o.user_id
FROM orders o
WHERE
  ($1 = 'ALL' OR o.current_status = $1)
  AND ($2 = 'ALL' OR o.current_stage = $2)
  AND (
    $3 = '%%'
    OR o.order_id ILIKE $3
    OR COALESCE(o.tracking_no,'') ILIKE $3
  )
ORDER BY o.updated_at DESC
LIMIT $4;


-- name: order_one
-- 주문 상세: shipping_address/user_id는 orders에 있으므로 orders에서 직접 조회
SELECT
  o.order_id,
  o.product_id,
  o.product_name,
  o.current_stage,
  o.current_status,
  o.hold_reason_code,
  o.last_event_type,
  o.last_occurred_at,
  o.tracking_no,
  o.promised_delivery_date,
  o.updated_at,
  COALESCE(o.hold_ops_status, 'OPEN') AS hold_ops_status,
  o.hold_ops_note,
  o.hold_ops_operator,
  o.hold_ops_updated_at,
  o.shipping_address,
  o.user_id
FROM orders o
WHERE o.order_id = $1;


-- name: order_events_by_order
-- events에는 shipping_address/user_id 컬럼이 없으니 orders를 조인해서 함께 내려줌
SELECT
  e.event_id,
  e.order_id,
  e.event_type,
  e.reason_code,
  e.occurred_at,
  e.ingested_at,
  e.source,
  o.shipping_address,
  o.user_id
FROM events e
LEFT JOIN orders o ON o.order_id = e.order_id
WHERE e.order_id = $1
ORDER BY e.occurred_at DESC
LIMIT $2;


-- name: events_page
-- events에는 shipping_address/user_id 컬럼이 없으니 orders를 조인해서 함께 내려줌
SELECT
  e.event_id,
  e.order_id,
  e.event_type,
  e.reason_code,
  e.occurred_at,
  e.ingested_at,
  e.source,
  o.shipping_address,
  o.user_id
FROM events e
LEFT JOIN orders o ON o.order_id = e.order_id
WHERE
  (COALESCE(e.order_id,'') ILIKE $1)
  AND (e.event_type ILIKE $2)
  AND (CASE WHEN $3 THEN (e.reason_code IS NOT NULL AND e.reason_code <> '') ELSE TRUE END)
ORDER BY e.occurred_at DESC
LIMIT $4;


-- name: alerts
SELECT *
FROM (
  -- EVENT 알림: reason_code 있는 이벤트
  -- shipping_address/user_id는 orders에서 조인해서 가져옴
  SELECT
    'EVENT'::text AS alert_kind,
    ('event:' || e.event_id)::text AS alert_key,
    e.event_id,
    e.order_id,
    e.event_type,
    e.reason_code,
    e.occurred_at,
    e.source,
    o.shipping_address,
    o.user_id,
    COALESCE(e.ops_status, 'OPEN') AS ops_status,
    e.ops_note,
    e.ops_operator,
    e.ops_updated_at
  FROM events e
  LEFT JOIN orders o ON o.order_id = e.order_id
  WHERE e.reason_code IS NOT NULL AND e.reason_code <> ''

  UNION ALL

  -- HOLD 알림: 현재 HOLD 상태(스냅샷)
  SELECT
    'HOLD'::text AS alert_kind,
    ('hold:' || o.order_id)::text AS alert_key,
    NULL::text AS event_id,
    o.order_id,
    'ORDER_HOLD'::text AS event_type,
    o.hold_reason_code AS reason_code,
    o.updated_at AS occurred_at,
    'orders'::text AS source,
    o.shipping_address,
    o.user_id,
    COALESCE(o.hold_ops_status, 'OPEN') AS ops_status,
    o.hold_ops_note AS ops_note,
    o.hold_ops_operator AS ops_operator,
    o.hold_ops_updated_at AS ops_updated_at
  FROM orders o
  WHERE o.hold_reason_code IS NOT NULL AND o.hold_reason_code <> ''
) x
ORDER BY x.occurred_at DESC
LIMIT $1;


-- name: alerts_page
SELECT *
FROM (
  SELECT
    'EVENT'::text AS alert_kind,
    ('event:' || e.event_id)::text AS alert_key,
    e.event_id,
    e.order_id,
    e.event_type,
    e.reason_code,
    e.occurred_at,
    e.source,
    o.shipping_address,
    o.user_id,
    COALESCE(e.ops_status, 'OPEN') AS ops_status,
    e.ops_note,
    e.ops_operator,
    e.ops_updated_at
  FROM events e
  LEFT JOIN orders o ON o.order_id = e.order_id
  WHERE e.reason_code IS NOT NULL AND e.reason_code <> ''

  UNION ALL

  SELECT
    'HOLD'::text AS alert_kind,
    ('hold:' || o.order_id)::text AS alert_key,
    NULL::text AS event_id,
    o.order_id,
    'ORDER_HOLD'::text AS event_type,
    o.hold_reason_code AS reason_code,
    o.updated_at AS occurred_at,
    'orders'::text AS source,
    o.shipping_address,
    o.user_id,
    COALESCE(o.hold_ops_status, 'OPEN') AS ops_status,
    o.hold_ops_note AS ops_note,
    o.hold_ops_operator AS ops_operator,
    o.hold_ops_updated_at AS ops_updated_at
  FROM orders o
  WHERE o.hold_reason_code IS NOT NULL AND o.hold_reason_code <> ''
) x
WHERE
  ($1 = 'ALL' OR x.ops_status = $1)
  AND ($2 = 'ALL' OR x.alert_kind = $2)
ORDER BY x.occurred_at DESC
LIMIT $3;


-- name: alerts_by_order
SELECT *
FROM (
  SELECT
    'EVENT'::text AS alert_kind,
    ('event:' || e.event_id)::text AS alert_key,
    e.event_id,
    e.order_id,
    e.event_type,
    e.reason_code,
    e.occurred_at,
    e.source,
    o.shipping_address,
    o.user_id,
    COALESCE(e.ops_status, 'OPEN') AS ops_status,
    e.ops_note,
    e.ops_operator,
    e.ops_updated_at
  FROM events e
  LEFT JOIN orders o ON o.order_id = e.order_id
  WHERE e.order_id = $1
    AND e.reason_code IS NOT NULL AND e.reason_code <> ''

  UNION ALL

  SELECT
    'HOLD'::text AS alert_kind,
    ('hold:' || o.order_id)::text AS alert_key,
    NULL::text AS event_id,
    o.order_id,
    'ORDER_HOLD'::text AS event_type,
    o.hold_reason_code AS reason_code,
    o.updated_at AS occurred_at,
    'orders'::text AS source,
    o.shipping_address,
    o.user_id,
    COALESCE(o.hold_ops_status, 'OPEN') AS ops_status,
    o.hold_ops_note AS ops_note,
    o.hold_ops_operator AS ops_operator,
    o.hold_ops_updated_at AS ops_updated_at
  FROM orders o
  WHERE o.order_id = $1
    AND o.hold_reason_code IS NOT NULL AND o.hold_reason_code <> ''
) x
ORDER BY x.occurred_at DESC
LIMIT $2;


-- name: events_ops_update
UPDATE events
SET
  ops_status = $2,
  ops_note = $3,
  ops_operator = $4,
  ops_updated_at = NOW()
WHERE event_id = $1;


-- name: order_hold_ops_update
UPDATE orders
SET
  hold_ops_status = $2,
  hold_ops_note = $3,
  hold_ops_operator = $4,
  hold_ops_updated_at = NOW(),
  updated_at = NOW()
WHERE order_id = $1;


-- name: clear_order_hold
UPDATE orders
SET
  hold_reason_code = NULL,
  updated_at = NOW()
WHERE order_id = $1;


-- name: order_current_update_status
UPDATE orders
SET
  current_status = $2,
  last_event_type = 'STATUS_CHANGED_MANUAL',
  last_occurred_at = NOW(),
  updated_at = NOW()
WHERE order_id = $1;


-- name: insert_manual_status_event
INSERT INTO orders(
  order_id, product_id, product_name,
  current_stage, current_status,
  hold_reason_code,
  last_event_type, last_occurred_at,
  tracking_no, promised_delivery_date,
  shipping_address, user_id,
  updated_at
)
VALUES (
  $1,$2,$3,
  $4,$5,
  $6,
  $7,$8,
  $9,$10,
  $11,$12,
  NOW()
)
ON CONFLICT (order_id) DO UPDATE
SET
  product_id = EXCLUDED.product_id,
  product_name = EXCLUDED.product_name,
  current_stage = EXCLUDED.current_stage,
  current_status = EXCLUDED.current_status,
  hold_reason_code = EXCLUDED.hold_reason_code,
  last_event_type = EXCLUDED.last_event_type,
  last_occurred_at = EXCLUDED.last_occurred_at,
  tracking_no = EXCLUDED.tracking_no,
  promised_delivery_date = EXCLUDED.promised_delivery_date,
  shipping_address = EXCLUDED.shipping_address,
  user_id = EXCLUDED.user_id,
  updated_at = NOW()
WHERE
  orders.last_occurred_at <= EXCLUDED.last_occurred_at;


-- name: event_update_latest_by_order
WITH t AS (
  SELECT event_id
  FROM events
  WHERE order_id = $1
  ORDER BY occurred_at DESC NULLS LAST, ingested_at DESC NULLS LAST
  LIMIT 1
)
UPDATE events e
SET
  event_type  = $2,
  reason_code = $3,
  occurred_at = NOW(),
  ingested_at = NOW(),
  source      = $4
FROM t
WHERE e.event_id = t.event_id;



-- name: event_insert_for_order
INSERT INTO events (
  event_id, order_id, event_type, reason_code,
  occurred_at, ingested_at, source
)
VALUES ($1, $2, $3, $4, NOW(), NOW(), $5);


