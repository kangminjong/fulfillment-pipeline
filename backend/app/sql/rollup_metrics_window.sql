WITH w AS (
  SELECT
    -- 현재 시각을 "15분 경계로 내림"한 시각이 window_end
    date_trunc('hour', now())
      + (extract(minute from now())::int / 15) * interval '15 minutes' AS window_end,

    -- window_start는 window_end - 15분
    (date_trunc('hour', now())
      + (extract(minute from now())::int / 15) * interval '15 minutes') - interval '15 minutes' AS window_start
),

-- ✅ 비즈니스 이벤트 발생량(occurred_at 기준)
ev AS (
  SELECT
    COUNT(*) FILTER (WHERE event_type IN ('ORDER_CREATED','CREATED')) AS orders,
    COUNT(*) FILTER (WHERE event_type IN ('PAYMENT_COMPLETED','PAID')) AS payments,
    COUNT(*) FILTER (WHERE event_type IN ('SHIPPED','ORDER_SHIPPED')) AS shipped,
    COUNT(*) FILTER (WHERE event_type IN ('HOLD','ORDER_HOLD')) AS holds
  FROM events
  WHERE occurred_at >= (SELECT window_start FROM w)
    AND occurred_at <  (SELECT window_end   FROM w)
),

-- ✅ 파이프라인 지표(ingested_at 기준)
-- 현재 스키마엔 parse_ok/schema_ok 같은 명시적 컬럼이 없으니
-- 1) 당장은 "키워드 기반"으로 세거나
-- 2) 그냥 0으로 고정하는 방식 중 선택하면 됨.
ing AS (
  SELECT
    COUNT(*) AS ingest_count,

    -- (옵션) event_type/reason_code에 'parse'가 들어가는 이벤트를 파싱 오류로 취급
    COUNT(*) FILTER (
      WHERE (event_type ILIKE '%parse%' OR COALESCE(reason_code,'') ILIKE '%parse%')
    ) AS parse_errors,

    -- (옵션) event_type/reason_code에 'schema'/'missing'가 들어가면 스키마 누락으로 취급
    COUNT(*) FILTER (
      WHERE (event_type ILIKE '%schema%'
             OR COALESCE(reason_code,'') ILIKE '%schema%'
             OR COALESCE(reason_code,'') ILIKE '%missing%')
    ) AS schema_missing
  FROM events
  WHERE ingested_at >= (SELECT window_start FROM w)
    AND ingested_at <  (SELECT window_end   FROM w)
)

INSERT INTO metrics_window (
  window_start, window_end,
  orders, payments, shipped, holds,
  hold_rate, ingest_count, parse_errors, schema_missing
)
SELECT
  w.window_start, w.window_end,
  ev.orders, ev.payments, ev.shipped, ev.holds,
  CASE
    WHEN ev.orders > 0 THEN ROUND((ev.holds::numeric / ev.orders::numeric), 3)
    ELSE 0
  END AS hold_rate,
  ing.ingest_count, ing.parse_errors, ing.schema_missing
FROM w
CROSS JOIN ev
CROSS JOIN ing
ON CONFLICT (window_start, window_end) DO UPDATE SET
  orders = EXCLUDED.orders,
  payments = EXCLUDED.payments,
  shipped = EXCLUDED.shipped,
  holds = EXCLUDED.holds,
  hold_rate = EXCLUDED.hold_rate,
  ingest_count = EXCLUDED.ingest_count,
  parse_errors = EXCLUDED.parse_errors,
  schema_missing = EXCLUDED.schema_missing,
  created_at = now();
