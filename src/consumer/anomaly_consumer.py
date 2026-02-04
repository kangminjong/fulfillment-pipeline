import os
import json
import uuid
import psycopg2
from datetime import datetime
from collections import deque, defaultdict
from kafka import KafkaConsumer

# ---------------------------------------------------------
# ⚙️ DB 및 Kafka 설정 (risk_consumer.py 스타일 유지)
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
# 🧠 시나리오 1: 인기상품 폭주(다수 유저가 같은 상품을 초단위로 주문 폭탄)
# - producer: 10~20건을 0.02초 간격으로 쏨
# - 탐지: "같은 product_id 주문이 WINDOW 내 THRESHOLD 이상"
# ---------------------------------------------------------
BURST_WINDOW_SEC = float(os.getenv("BURST_WINDOW_SEC", "1.0"))          # 1초 창
BURST_THRESHOLD = int(os.getenv("BURST_THRESHOLD", "10"))              # 10건 이상이면 폭주로 판단
product_rate_tracker = defaultdict(lambda: deque())  # {product_id: deque([datetime,...])}

# ---------------------------------------------------------
# 🧠 시나리오 3: 랜덤 재고 부족 유발
# - 탐지: products 테이블 stock <= 0 이면 HOLD
# ---------------------------------------------------------
STOCK_HOLD_THRESHOLD = int(os.getenv("STOCK_HOLD_THRESHOLD", "0"))      # 0 이하면 재고없음으로 HOLD

SQL_SELECT_STOCK = """
SELECT stock
FROM public.products
WHERE product_id = %s
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

    # 상품이 아예 없으면(데이터 불일치) -> 이것도 운영상 HOLD로 두는 게 안전
    if row is None:
        return True

    stock = row[0]
    return stock is not None and stock <= STOCK_HOLD_THRESHOLD


# ---------------------------------------------------------
# 💾 DB 저장 (risk_consumer.py 구조 유지)
# - 이상이면 orders.current_status = HOLD, hold_reason_code 저장
# - events에도 기록 (event_type = HOLD)
# ---------------------------------------------------------
def save_to_db(cur, data, final_status, hold_reason=None):
    cur.execute("""
        INSERT INTO public.orders (
            order_id, product_id, product_name,
            current_stage, current_status,
            hold_reason_code, last_event_type, last_occurred_at,
            shipping_address, user_id, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        data.get("order_id"),
        data.get("product_id"),
        data.get("product_name"),
        data.get("current_stage"),
        final_status,
        hold_reason,
        data.get("last_event_type"),
        parse_iso_datetime(data.get("last_occurred_at")),
        data.get("address"),
        data.get("customer_id"),
    ))

    cur.execute("""
        INSERT INTO public.events (
            event_id, order_id, event_type, reason_code,
            occurred_at, source, payload_json
        ) VALUES (%s, %s, %s, %s, NOW(), %s, %s)
    """, (
        str(uuid.uuid4()),
        data.get("order_id"),
        final_status,                 # PASS면 PAID/PICKING.. HOLD면 HOLD
        hold_reason,
        "ANOMALY_CONSUMER",
        json.dumps(data, ensure_ascii=False),
    ))


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
        enable_auto_commit=True,
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
                    # 보통 재고/폭주 판단은 "주문 발생/결제 완료(PAID)" 시점에서만 하는 게 자연스러움
                    if order.get("current_status") == "PAID":
                        # 1) 폭주 감지
                        is_burst = check_burst_anomaly(order)

                        # 3) 재고부족 감지
                        is_stockout = check_stock_anomaly(cur, order)

                        if is_stockout:
                            final_status = "HOLD"
                            hold_reason = "OUT_OF_STOCK"   # 시나리오 3
                        elif is_burst:
                            final_status = "HOLD"
                            hold_reason = "TRAFFIC_SPIKE"  # 시나리오 1

                    save_to_db(cur, order, final_status, hold_reason)
                    conn.commit()

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