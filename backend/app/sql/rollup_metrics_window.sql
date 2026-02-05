-- =========================================================
-- metrics_window upsert (30분 버킷 하나 계산해서 저장)
-- - window_start=$1, window_end=$2
-- - 정의:
--   * orders  = orders 테이블 "생성된 주문 수" (created_at 기준)
--   * events  = 주문 변경 로그
--   * HOLD    = 에러
--   * alerts  = HOLD(현재 HOLD 상태 주문)만
-- =========================================================
-- name: metrics_window_upsert_30m
WITH calc AS (
  SELECT
    $1::timestamptz AS window_start,
    $2::timestamptz AS window_end,

    -- ✅ 분모(전체 주문수): orders.created_at 기준
    (SELECT COUNT(*)
     FROM orders
     WHERE created_at >= $1 AND created_at < $2
    ) AS orders,

    -- Flow: 이벤트 로그(ingested_at 기준)
    (SELECT COUNT(*) FROM events
     WHERE current_status='PAID' AND ingested_at >= $1 AND ingested_at < $2
    ) AS payments,

    (SELECT COUNT(*) FROM events
     WHERE current_status='SHIPPED' AND ingested_at >= $1 AND ingested_at < $2
    ) AS shipped,

    -- 홀드 주문 수(중복 제거) + "윈도우 내 생성 주문"으로 제한
    (SELECT COUNT(DISTINCT e.order_id)
     FROM events e
     JOIN orders o ON o.order_id = e.order_id
     WHERE e.order_id IS NOT NULL
       AND o.current_status IN ('HOLD','ORDER_HOLD')
       AND e.ingested_at  >= $1 AND e.ingested_at  < $2
    ) AS holds,

    -- ingest_count: ingested_at 기준
    (SELECT COUNT(*) FROM events
     WHERE ingested_at >= $1 AND ingested_at < $2
    ) AS ingest_count,

    0::int AS parse_errors,
    0::int AS schema_missing,

    -- ✅ Snapshot 정의 통일 (summary와 맞춤)
    (SELECT COUNT(*) FROM orders WHERE current_status='PAID') AS backlog,
    (SELECT COUNT(*) FROM orders WHERE current_status='HOLD') AS holds_now,

    -- late_orders: 일단 "PAID가 오래된 것"으로(원하면 SLA 바꾸면 됨)
    (SELECT COUNT(*) FROM orders
     WHERE current_status='PAID'
       AND created_at < now() - interval '2 hours'
    ) AS late_orders,

    -- 이벤트 통계(윈도우 내): 파이프라인 관점이면 ingested_at 유지
    (SELECT COUNT(*) FROM events
     WHERE ingested_at >= $1 AND ingested_at < $2
    ) AS event_total,

    (SELECT COUNT(*) FROM events
     WHERE ingested_at >= $1 AND ingested_at < $2
       AND current_status ='HOLD'
    ) AS event_errors,

    COALESCE((
      SELECT percentile_cont(0.95) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))
      )
      FROM events
      WHERE ingested_at >= $1 AND ingested_at < $2
    ), 0) AS delay_p95_sec,

    -- Alerts snapshot (EVENT 오류 + HOLD 주문)
    (
      (SELECT COUNT(*) FROM events
        WHERE current_status='HOLD'
          AND COALESCE(ops_status,'OPEN')='OPEN' AND ingested_at >= $1 AND ingested_at < $2)
    ) AS alerts_open,

    (
      (SELECT COUNT(*) FROM events WHERE current_status='HOLD' AND ops_status='ACK' AND ingested_at >= $1 AND ingested_at < $2)
    ) AS alerts_ack,

    (
      (SELECT COUNT(*) FROM events WHERE current_status='HOLD' AND ops_status='RESOLVED' AND ingested_at >= $1 AND ingested_at < $2)
    ) AS alerts_resolved,

    (
      (SELECT COUNT(*) FROM events WHERE current_status='HOLD' AND ops_status='RETRY_REQUESTED' AND ingested_at >= $1 AND ingested_at < $2)
    ) AS alerts_retry
)
INSERT INTO metrics_window(
  window_start, window_end,
  orders, payments, shipped, holds,
  hold_rate,
  ingest_count, parse_errors, schema_missing,
  backlog, holds_now, late_orders,
  event_total, event_errors, delay_p95_sec,
  alerts_open, alerts_ack, alerts_resolved, alerts_retry
)
SELECT
  window_start, window_end,
  orders, payments, shipped, holds,
  COALESCE(holds::numeric / NULLIF(orders, 0), 0) AS hold_rate,
  ingest_count, parse_errors, schema_missing,
  backlog, holds_now, late_orders,
  event_total, event_errors, delay_p95_sec,
  alerts_open, alerts_ack, alerts_resolved, alerts_retry
FROM calc
ON CONFLICT (window_start, window_end)
DO UPDATE SET
  orders = EXCLUDED.orders,
  payments = EXCLUDED.payments,
  shipped = EXCLUDED.shipped,
  holds = EXCLUDED.holds,
  hold_rate = EXCLUDED.hold_rate,
  ingest_count = EXCLUDED.ingest_count,
  parse_errors = EXCLUDED.parse_errors,
  schema_missing = EXCLUDED.schema_missing,
  backlog = EXCLUDED.backlog,
  holds_now = EXCLUDED.holds_now,
  late_orders = EXCLUDED.late_orders,
  event_total = EXCLUDED.event_total,
  event_errors = EXCLUDED.event_errors,
  delay_p95_sec = EXCLUDED.delay_p95_sec,
  alerts_open = EXCLUDED.alerts_open,
  alerts_ack = EXCLUDED.alerts_ack,
  alerts_resolved = EXCLUDED.alerts_resolved,
  alerts_retry = EXCLUDED.alerts_retry,
  created_at = now();
