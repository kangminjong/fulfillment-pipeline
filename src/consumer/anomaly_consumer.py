import os
import json
import uuid
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
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

# (DB 구조 대응) orders_raw에 원본 저장 후 raw_id 확보
SQL_INSERT_ORDERS_RAW = """
INSERT INTO public.orders_raw (
    raw_payload,
    kafka_offset,
    ingested_at
) VALUES (%s, %s, NOW())
RETURNING raw_id;
"""

# (DB 구조 대응) orders 스냅샷 UPSERT (raw_reference_id NOT NULL + FK)
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
    hold_ops_user,
    hold_ops_comment,
    raw_reference_id,
    updated_at
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, NOW()
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
    hold_ops_user = EXCLUDED.hold_ops_user,
    hold_ops_comment = EXCLUDED.hold_ops_comment,
    raw_reference_id = EXCLUDED.raw_reference_id,
    updated_at = NOW();
"""

# (DB 구조 대응) events 원장 INSERT (source/payload_json 없음, current_status 필요)
SQL_INSERT_EVENTS = """
INSERT INTO public.events (
    event_id,
    order_id,
    event_type,
    current_status,
    reason_code,
    occurred_at,
    ingested_at,
    ops_user,
    ops_comment
) VALUES (
    %s, %s, %s, %s,
    %s, %s, NOW(),
    %s, %s
)
ON CONFLICT (event_id) DO NOTHING;
"""

# ---------------------------------------------------------
# 유틸
# ---------------------------------------------------------
def parse_iso_datetime(value: str) -> datetime:
    """producer가 보내는 ISO 문자열 파싱 (tz 없어도 처리)"""
    if not value:
        return datetime.now()
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.now()


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

    now_dt = parse_iso_datetime(order_data.get("last_occurred_at"))
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
# ---------------------------------------------------------
def save_to_db(cur, data, final_status, hold_reason=None, kafka_offset=None):
    # (DB 구조 대응) 0) 원본을 orders_raw에 먼저 저장하고 raw_id 확보
    raw_payload = dict(data)
    raw_payload["_meta"] = {
        "source": "ANOMALY_CONSUMER",
        "kafka_offset": kafka_offset,
        "final_status": final_status,
        "hold_reason": hold_reason,
    }

    cur.execute(
        SQL_INSERT_ORDERS_RAW,
        (Json(raw_payload), kafka_offset),
    )
    raw_id = cur.fetchone()[0]

    # (DB 구조 대응) 1) orders UPSERT (raw_reference_id 반드시 포함)
    order_id = data.get("order_id")
    product_id = data.get("product_id")
    product_name = data.get("product_name")
    current_stage = data.get("current_stage")
    last_event_type = data.get("last_event_type") or data.get("event_type") or data.get("current_status") or "UNKNOWN"
    last_occurred_at = parse_iso_datetime(data.get("last_occurred_at") or data.get("occurred_at"))

    # producer는 address 키를 쓰는 경우가 많음
    shipping_address = to_text_or_json(data.get("shipping_address") or data.get("address"))

    # producer는 customer_id → DB user_id
    user_id = data.get("user_id") or data.get("customer_id")

    # HOLD 자동 판정이면 ops_comment에 근거를 남겨두기
    hold_ops_user = "ANOMALY_CONSUMER" if final_status == "HOLD" else None
    hold_ops_comment = hold_reason if final_status == "HOLD" else None

    cur.execute(
        SQL_UPSERT_ORDERS,
        (
            order_id,
            user_id,
            product_id,
            product_name,
            shipping_address,
            current_stage,
            final_status,      # ✅ 스냅샷 상태는 최종 상태(HOLD/PASS)
            last_event_type,
            last_occurred_at,
            hold_reason,       # ✅ hold_reason_code
            hold_ops_user,
            hold_ops_comment,
            raw_id,            # ✅ raw_reference_id (NOT NULL + FK)
        ),
    )

    # (DB 구조 대응) 2) events INSERT (원장)
    # event_type은 HOLD가 명확하면 HOLD로, 아니면 원래 이벤트 타입을 보존
    event_type = "HOLD" if final_status == "HOLD" else last_event_type

    # events.current_status는 NOT NULL일 수 있으니 final_status 우선
    current_status_for_events = final_status or data.get("current_status") or "UNKNOWN"

    cur.execute(
        SQL_INSERT_EVENTS,
        (
            str(uuid.uuid4()),
            order_id,
            event_type,
            current_status_for_events,
            hold_reason,                 # reason_code
            last_occurred_at,            # occurred_at
            "ANOMALY_CONSUMER",          # ops_user
            json.dumps(raw_payload, ensure_ascii=False),  # ops_comment에 원본+메타 기록
        ),
    )


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
            final_status = order.get("current_status")
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
                    conn.commit()
                    consumer.commit()  # ✅ DB 커밋 성공 후에만 Kafka offset commit

                if final_status == "HOLD":
                    print(f"🛑 [HOLD] {order.get('product_id')} | {order.get('product_name')} | 사유: {hold_reason}")
                else:
                    print(f"✅ [PASS] {final_status} | {order.get('product_name')}")

            except Exception as e:
                conn.rollback()
                print(f"🔥 DB Error: {e}")

    except KeyboardInterrupt:
        conn.close()
        consumer.close()
        print("\n🛑 anomaly_consumer 종료")
