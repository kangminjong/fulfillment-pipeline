import os
import json
import uuid
import time
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone
from collections import deque, defaultdict
from kafka import KafkaConsumer

# ---------------------------------------------------------
# ⚙️ DB 및 Kafka 설정
# ---------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "192.168.239.40"),
    "database": os.getenv("POSTGRES_DB", "fulfillment"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "event")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomaly-detection-group")

# ---------------------------------------------------------
# ✅ 팀 에러 코드(Reason Code) 표준
# ---------------------------------------------------------
REASON_OOS = "FUL-INV"               # 재고 부족
REASON_PROD_FRAUD = "FUL-FRAUD-PROD" # 상품 기준 이상거래(폭주 등)

# ---------------------------------------------------------
# 🧠 시나리오 1: 인기상품 폭주(다수 유저가 같은 상품을 초단위로 주문 폭탄)
# - producer: 10~20건을 0.02초 간격으로 쏨
# - 탐지: "같은 product_id 주문이 WINDOW 내 THRESHOLD 이상"
# ---------------------------------------------------------
BURST_WINDOW_SEC = float(os.getenv("BURST_WINDOW_SEC", "1.0"))  # 1초 창
BURST_THRESHOLD = int(os.getenv("BURST_THRESHOLD", "10"))       # 10건 이상이면 폭주로 판단
product_rate_tracker = defaultdict(lambda: deque())             # {product_id: deque([datetime,...])}

# ---------------------------------------------------------
# 🧠 시나리오 3: 랜덤 재고 부족 유발
# - 탐지: products 테이블 stock <= 0 이면 HOLD
# ---------------------------------------------------------
STOCK_HOLD_THRESHOLD = int(os.getenv("STOCK_HOLD_THRESHOLD", "0"))  # 0 이하면 재고없음으로 HOLD

SQL_SELECT_STOCK = """
SELECT stock
FROM public.products
WHERE product_id = %s
"""

# ---------------------------------------------------------
# ✅ (DB 구조 대응) orders_raw → events(원장) → orders(스냅샷)
#   - orders_raw: 원본(raw_payload) 먼저 저장해서 raw_id 확보
#   - events: 가능하면 항상 저장(원장)
#   - orders: 스냅샷 upsert (실패해도 events/raw는 남기기 위해 SAVEPOINT)
# ---------------------------------------------------------

# (DB 구조 대응) orders_raw에 원본 저장 후 raw_id 확보
SQL_INSERT_ORDERS_RAW = """
INSERT INTO public.orders_raw (
    raw_payload,
    kafka_offset,
    ingested_at
) VALUES (%s, %s, NOW())
RETURNING raw_id;
"""

# (DB 구조 대응) events 원장 INSERT
# ✅ 최신 events 컬럼: ops_status, ops_note, ops_operator, ops_updated_at
SQL_INSERT_EVENTS = """
INSERT INTO public.events (
    event_id,
    order_id,
    event_type,
    current_status,
    reason_code,
    occurred_at,
    ingested_at,
    ops_status,
    ops_note,
    ops_operator,
    ops_updated_at
) VALUES (
    %s, %s, %s, %s,
    %s, %s, NOW(),
    %s, %s, %s, %s
)
ON CONFLICT (event_id) DO NOTHING;
"""

# (DB 구조 대응) orders 스냅샷 UPSERT
# ✅ 최신 orders 컬럼: hold_ops_status/hold_ops_note/hold_ops_operator/hold_ops_updated_at
# ✅ updated_at 컬럼 없음 (DDL 기준)
# ✅ created_at은 DEFAULT now()라 INSERT에 넣지 않음
SQL_UPSERT_ORDERS = """
INSERT INTO public.orders (
    order_id,
    user_id,
    product_id,
    product_name,
    shipping_address,
    current_stage,
    current_status,
    last_event_type,
    last_occurred_at,
    hold_reason_code,
    hold_ops_status,
    hold_ops_note,
    hold_ops_operator,
    hold_ops_updated_at,
    raw_reference_id
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (order_id)
DO UPDATE SET
    user_id = EXCLUDED.user_id,
    product_id = EXCLUDED.product_id,
    product_name = EXCLUDED.product_name,
    shipping_address = EXCLUDED.shipping_address,
    current_stage = EXCLUDED.current_stage,
    current_status = EXCLUDED.current_status,
    last_event_type = EXCLUDED.last_event_type,
    last_occurred_at = EXCLUDED.last_occurred_at,
    hold_reason_code = EXCLUDED.hold_reason_code,
    hold_ops_status = EXCLUDED.hold_ops_status,
    hold_ops_note = EXCLUDED.hold_ops_note,
    hold_ops_operator = EXCLUDED.hold_ops_operator,
    hold_ops_updated_at = EXCLUDED.hold_ops_updated_at,
    raw_reference_id = EXCLUDED.raw_reference_id;
"""

# ---------------------------------------------------------
# 유틸
# ---------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value) -> datetime:
    """producer가 보내는 ISO 문자열 파싱 (tz 없어도 처리)"""
    if not value:
        return now_utc()

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        v = value.strip()
        try:
            # "Z" 대응
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return now_utc()

    return now_utc()


def to_text_or_json(value):
    """
    text 컬럼에 dict/list가 들어오면 오류날 수 있음.
    - dict/list -> JSON 문자열
    - 기타 -> str
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


# ---------------------------------------------------------
# ⚖️ 이상 판단 로직 (시나리오 1 + 3)
# ---------------------------------------------------------
def check_burst_anomaly(order_data) -> bool:
    """
    같은 상품(product_id)에 대해 WINDOW_SEC 안에 THRESHOLD 이상 주문이 들어오면 폭주로 판단.
    """
    pid = order_data.get("product_id")
    if not pid:
        return False

    now_dt = parse_iso_datetime(order_data.get("last_occurred_at") or order_data.get("occurred_at"))
    q = product_rate_tracker[pid]
    q.append(now_dt)

    # WINDOW 밖은 제거
    cutoff = now_dt.timestamp() - BURST_WINDOW_SEC
    while q and q[0].timestamp() < cutoff:
        q.popleft()

    return len(q) >= BURST_THRESHOLD


def check_stock_anomaly(cur, order_data) -> bool:
    """
    products.stock 조회해서 STOCK_HOLD_THRESHOLD 이하이면 재고부족으로 판단.
    """
    pid = order_data.get("product_id")
    if not pid:
        return False

    cur.execute(SQL_SELECT_STOCK, (pid,))
    row = cur.fetchone()

    # 상품이 아예 없으면(데이터 불일치) -> 운영상 HOLD로 두는 게 안전 (재고 문제로 취급)
    if row is None:
        return True

    stock = row[0]
    return stock is not None and stock <= STOCK_HOLD_THRESHOLD


# ---------------------------------------------------------
# 💾 DB 저장 (risk_consumer.py 구조 유지)
# - 이상이면 orders.current_status = HOLD, hold_reason_code 저장
# - events에도 기록 (event_type = HOLD)
#
# ✅ 최신 DB 흐름:
# 0) orders_raw insert → raw_id 확보
# 1) events insert (원장: 가능하면 항상 저장)
# 2) orders upsert (스냅샷: SAVEPOINT로 실패해도 events/raw는 남김)
# ---------------------------------------------------------
def save_to_db(cur, data, final_status, hold_reason=None, kafka_offset=None):
    # (DB 구조 대응) 0) 원본을 orders_raw에 먼저 저장하고 raw_id 확보
    raw_payload = dict(data)
    raw_payload["_meta"] = {
        "source": "ANOMALY_CONSUMER",
        "kafka_offset": kafka_offset,
        "final_status": final_status,
        "hold_reason": hold_reason,
        "ingested_at": now_utc().isoformat(),
    }

    cur.execute(
        SQL_INSERT_ORDERS_RAW,
        (Json(raw_payload), kafka_offset),
    )
    raw_id = cur.fetchone()[0]

    # 공통 필드 정규화
    order_id = data.get("order_id")
    product_id = data.get("product_id")
    product_name = data.get("product_name")
    current_stage = data.get("current_stage")

    # producer는 customer_id → DB user_id
    user_id = data.get("user_id") or data.get("customer_id")

    # producer는 address 키를 쓰는 경우가 많음
    shipping_address = to_text_or_json(data.get("shipping_address") or data.get("address"))

    last_event_type = (
        data.get("last_event_type")
        or data.get("event_type")
        or data.get("current_status")
        or "UNKNOWN"
    )
    last_occurred_at = parse_iso_datetime(data.get("last_occurred_at") or data.get("occurred_at"))

    # ---------------------------------------------------------
    # (DB 구조 대응) 1) events INSERT (원장)
    # - event_type: HOLD면 HOLD로 명시, 아니면 원래 이벤트 타입 보존
    # - current_status: final_status 우선 (HOLD/PASS/PAID 등)
    # - ops_*: anomaly consumer가 남기는 운영 메타
    # ---------------------------------------------------------
    event_type_for_events = "HOLD" if final_status == "HOLD" else last_event_type
    current_status_for_events = final_status or data.get("current_status") or "UNKNOWN"

    # events의 ops_*는 “운영 상태/메모/담당/시각” 느낌으로 남기기
    ops_status = "AUTO_HOLD" if final_status == "HOLD" else "AUTO_PASS"
    ops_note = hold_reason if final_status == "HOLD" else None
    ops_operator = "ANOMALY_CONSUMER"
    ops_updated_at = now_utc()

    cur.execute(
        SQL_INSERT_EVENTS,
        (
            str(uuid.uuid4()),         # event_id
            order_id,
            event_type_for_events,
            current_status_for_events,  # current_status
            hold_reason,                # reason_code
            last_occurred_at,           # occurred_at
            ops_status,
            # ops_note: 너무 길면 부담이니, 기본은 hold_reason / 필요하면 raw_payload를 요약해서 넣기
            ops_note or json.dumps({"note": "auto decision", "meta": raw_payload.get("_meta")}, ensure_ascii=False),
            ops_operator,
            ops_updated_at,
        ),
    )

    # ---------------------------------------------------------
    # (DB 구조 대응) 2) orders UPSERT (스냅샷) - SAVEPOINT
    # - orders는 NOT NULL이 많아서, 여기서 실패할 수 있음
    # - 실패해도 raw/events는 남겨야 하므로 SAVEPOINT로 감싼다
    # ---------------------------------------------------------
    cur.execute("SAVEPOINT sp_orders;")
    try:
        missing = []
        if not order_id:
            missing.append("order_id")
        if not user_id:
            missing.append("user_id")
        if not product_id:
            missing.append("product_id")
        if not product_name:
            missing.append("product_name")
        if not shipping_address:
            missing.append("shipping_address")
        if not current_stage:
            missing.append("current_stage")
        if not current_status_for_events:
            missing.append("current_status")
        if not last_event_type:
            missing.append("last_event_type")
        if not last_occurred_at:
            missing.append("last_occurred_at")

        # orders는 필수값 누락이면 스냅샷 스킵 (원장은 이미 저장됨)
        if missing:
            print(f"⚠️ [SKIP orders upsert] 필수값 누락: {', '.join(missing)} (order_id={order_id})")
            cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
            return

        # HOLD 자동 판정이면 hold_ops_*에 자동조치 흔적 남기기
        hold_ops_status = "PENDING_REVIEW" if final_status == "HOLD" else None
        hold_ops_note = hold_reason if final_status == "HOLD" else None
        hold_ops_operator = "ANOMALY_CONSUMER" if final_status == "HOLD" else None
        hold_ops_updated_at = now_utc() if final_status == "HOLD" else None

        cur.execute(
            SQL_UPSERT_ORDERS,
            (
                order_id,
                user_id,
                product_id,
                product_name,
                shipping_address,
                current_stage,
                current_status_for_events,  # ✅ 스냅샷 상태는 최종 상태(HOLD/PASS/PAID...)
                last_event_type,
                last_occurred_at,
                hold_reason,                # hold_reason_code
                hold_ops_status,
                hold_ops_note,
                hold_ops_operator,
                hold_ops_updated_at,
                raw_id,                     # raw_reference_id (NOT NULL + FK)
            ),
        )

    except Exception as e_orders:
        cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
        print(f"⚠️ [orders upsert 실패 - raw/events는 저장됨] order_id={order_id} err={e_orders}")


# ---------------------------------------------------------
# 🚀 메인
# ---------------------------------------------------------
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="latest",
        enable_auto_commit=False,  # ✅ DB commit 성공 후에만 offset commit
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("📡 [Anomaly Consumer] 시나리오 1(폭주), 3(재고부족) 감지 가동 중...")

    try:
        for message in consumer:
            order = message.value

            # 기본은 원래 상태로 통과
            final_status = order.get("current_status") or "UNKNOWN"
            hold_reason = None

            try:
                with conn.cursor() as cur:
                    # 보통 재고/폭주 판단은 "결제 완료(PAID)" 시점에서만 하는 게 자연스러움
                    if order.get("current_status") == "PAID":
                        # 1) 폭주 감지
                        is_burst = check_burst_anomaly(order)

                        # 3) 재고부족 감지
                        is_stockout = check_stock_anomaly(cur, order)

                        if is_stockout:
                            final_status = "HOLD"
                            hold_reason = REASON_OOS          # ✅ FUL-INV
                        elif is_burst:
                            final_status = "HOLD"
                            hold_reason = REASON_PROD_FRAUD   # ✅ FUL-FRAUD-PROD

                    save_to_db(cur, order, final_status, hold_reason, kafka_offset=message.offset)

                    # ✅ 트랜잭션 커밋이 성공해야 offset도 커밋
                    conn.commit()
                    consumer.commit()

                if final_status == "HOLD":
                    print(f"🛑 [HOLD] {order.get('product_id')} | {order.get('product_name')} | 사유: {hold_reason}")
                else:
                    print(f"✅ [PASS] {final_status} | {order.get('product_name')}")

            except Exception as e:
                conn.rollback()
                print(f"🔥 DB Error: {e}")

    except KeyboardInterrupt:
        try:
            conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        print("\n🛑 anomaly_consumer 종료")