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
# ⚙️ DB 및 Kafka 설정 (consumer.py / risk_consumer와 통일)
# ---------------------------------------------------------
# ✅ 환경변수 기반 (docker-compose 기준)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomaly-detection-group")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")  # anomaly는 운영상 latest가 자연스러움

# ✅ 팀 DB 접속 규칙: localhost 사용 안 함 (기본값은 팀 DB IP로)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.239.40")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

DB_CONFIG = {
    "host": POSTGRES_HOST,
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "port": POSTGRES_PORT,
}

# ---------------------------------------------------------
# ✅ 팀 에러 코드(Reason Code) 표준
# ---------------------------------------------------------
REASON_OOS = "FUL-INV"               # 재고 부족
REASON_PROD_FRAUD = "FUL-FRAUD-PROD" # 상품 기준 이상거래(폭주 등)

# ---------------------------------------------------------
# 🧠 시나리오 1: 인기상품 폭주(다수 유저가 같은 상품을 초단위로 주문 폭탄)
# - producer: 동일 상품 주문을 "최대 6건" 정도만 보냄
# - 탐지 목표: "주문 3개까지 정상, 4번째부터 이상(HOLD)"
#
# ✅ 즉, WINDOW 내 동일 product_id 이벤트가 4개 이상이면 폭주로 판단
# ---------------------------------------------------------
BURST_WINDOW_SEC = float(os.getenv("BURST_WINDOW_SEC", "1.0"))  # 1초 창
BURST_THRESHOLD = int(os.getenv("BURST_THRESHOLD", "4"))        # ✅ 4건 이상이면 폭주로 판단(4번째부터 HOLD)
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
# ✅ (현재 anomaly_consumer에서는 ops_*는 저장하지 않음)
SQL_INSERT_EVENTS = """
INSERT INTO public.events (
    event_id,
    order_id,
    event_type,
    current_status,
    reason_code,
    occurred_at,
    ingested_at
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (event_id) DO NOTHING;
"""

# (DB 구조 대응) orders 스냅샷 UPSERT
# ✅ 최신 orders 컬럼: hold_ops_status/hold_ops_note/hold_ops_operator/hold_ops_updated_at
# ✅ created_at은 DEFAULT now()라 INSERT에 넣지 않음
# ✅ (현재 anomaly_consumer에서는 hold_ops_*는 저장하지 않음)
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
    raw_reference_id
) VALUES (
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
    raw_reference_id = EXCLUDED.raw_reference_id;
"""

# ---------------------------------------------------------
# ✅ HOLD / 후속 이벤트에서 필수값 누락 시 orders에서 보강 조회
# (consumer.py에서 쓰는 방식과 동일)
# ---------------------------------------------------------
SQL_SELECT_FROM_ORDERS = """
SELECT user_id, product_id, product_name, shipping_address
FROM public.orders
WHERE order_id = %s
LIMIT 1;
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


def stable_event_id(order_id: str, event_type: str, occurred_at: datetime) -> str:
    """
    producer가 event_id를 안 주는 경우, 재처리에도 중복 insert 줄이기 위한 결정적 UUID
    (consumer.py와 동일한 철학)
    """
    if not order_id:
        return str(uuid.uuid4())
    base = f"{order_id}|{event_type}|{occurred_at.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


# ---------------------------------------------------------
# ✅ DB 연결 (재시도)
# ---------------------------------------------------------
def connect_db_with_retry():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            print("✅ Postgres 연결 성공")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# ---------------------------------------------------------
# ⚖️ 이상 판단 로직 (시나리오 1 + 3)
# ---------------------------------------------------------
def check_burst_anomaly(order_data) -> bool:
    """
    같은 상품(product_id)에 대해 WINDOW_SEC 안에 THRESHOLD 이상 주문이 들어오면 폭주로 판단.
    ✅ 현재 목표: 4번째부터 이상 → THRESHOLD=4
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

    # (선택) 메모리 보호: 비정상 상황에서 deque가 너무 커지는 것 방지
    if len(q) > 5000:
        while len(q) > 5000:
            q.popleft()

    # ✅ 4개 이상이면 이상(4번째부터 HOLD)
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
# 💾 DB 저장 (consumer.py / risk_consumer 정책을 DB 구조에 맞춰 통일)
#
# - 이상이면 orders.current_status = HOLD, hold_reason_code 저장
# - events에도 기록 (event_type = HOLD)
#
# ✅ 최신 DB 흐름:
# 0) orders_raw insert → raw_id 확보
# 1) events insert (원장: 가능하면 항상 저장)
# 2) orders upsert (스냅샷: SAVEPOINT로 실패해도 events/raw는 남김)
# ---------------------------------------------------------
def save_to_db(cur, data, final_status, hold_reason=None, kafka_offset=None):
    ingested_at = now_utc()

    # (DB 구조 대응) 0) 원본을 orders_raw에 먼저 저장하고 raw_id 확보
    raw_payload = dict(data)
    raw_payload["_meta"] = {
        "source": "ANOMALY_CONSUMER",
        "kafka_offset": kafka_offset,
        "final_status": final_status,
        "hold_reason": hold_reason,
        "ingested_at": ingested_at.isoformat(),
    }

    cur.execute(
        SQL_INSERT_ORDERS_RAW,
        (Json(raw_payload), kafka_offset),
    )
    raw_id = cur.fetchone()[0]

    # 공통 필드 정규화 (producer 스키마 혼재 대응: customer_id/address)
    order_id = data.get("order_id")
    current_stage = data.get("current_stage")

    user_id = data.get("user_id") or data.get("customer_id")
    product_id = data.get("product_id")
    product_name = data.get("product_name")
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

    # ✅ event_id: 재처리 중복 방지
    event_id = data.get("event_id") or stable_event_id(order_id, event_type_for_events, last_occurred_at)

    cur.execute(
        SQL_INSERT_EVENTS,
        (
            event_id,
            order_id,
            event_type_for_events,
            current_status_for_events,
            hold_reason,        # reason_code
            last_occurred_at,   # occurred_at
            ingested_at,        # ingested_at
        ),
    )

    # ---------------------------------------------------------
    # (DB 구조 대응) 2) orders UPSERT (스냅샷) - SAVEPOINT
    # ---------------------------------------------------------
    cur.execute("SAVEPOINT sp_orders;")
    try:
        # HOLD 등에서 user/product/address 누락될 수 있어 기존 스냅샷으로 보강
        if order_id and (not user_id or not product_id or not product_name or not shipping_address):
            cur.execute(SQL_SELECT_FROM_ORDERS, (order_id,))
            row = cur.fetchone()
            if row:
                existing_user_id, existing_product_id, existing_product_name, existing_shipping_address = row
                user_id = user_id or existing_user_id
                product_id = product_id or existing_product_id
                product_name = product_name or existing_product_name
                shipping_address = shipping_address or existing_shipping_address

        missing = []
        if not order_id:
            missing.append("order_id")
        if not current_stage:
            missing.append("current_stage")
        if not current_status_for_events:
            missing.append("current_status")

        if not user_id:
            missing.append("user_id")
        if not product_id:
            missing.append("product_id")
        if not product_name:
            missing.append("product_name")
        if not shipping_address:
            missing.append("shipping_address")

        if missing:
            print(f"⚠️ [SKIP orders upsert] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
            cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
            return

        cur.execute(
            SQL_UPSERT_ORDERS,
            (
                order_id,
                user_id,
                product_id,
                product_name,
                shipping_address,
                current_stage,
                current_status_for_events,
                last_event_type,
                last_occurred_at,
                hold_reason,  # hold_reason_code
                raw_id,
            ),
        )

    except Exception as e_orders:
        cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
        print(f"⚠️ [orders upsert 실패 - raw/events는 저장됨] event_id={event_id} order_id={order_id} err={e_orders}")


# ---------------------------------------------------------
# 🚀 메인
# ---------------------------------------------------------
def main():
    print("📡 [Anomaly Consumer] 시나리오 1(폭주), 3(재고부족) 감지 가동 중...")
    print("=" * 60)
    print(f"- topic      : {KAFKA_TOPIC}")
    print(f"- bootstrap  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"- group_id   : {KAFKA_GROUP_ID}")
    print(f"- offset     : {AUTO_OFFSET_RESET}")
    print(f"- burst_win  : {BURST_WINDOW_SEC}s")
    print(f"- burst_th   : {BURST_THRESHOLD} (✅ 4번째부터 이상)")
    print("=" * 60)

    conn = connect_db_with_retry()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,  # ✅ DB commit 성공 후에만 offset commit
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    try:
        for message in consumer:
            order = message.value if isinstance(message.value, dict) else {}

            # 기본은 원래 상태로 통과
            final_status = order.get("current_status") or "UNKNOWN"
            hold_reason = None

            try:
                with conn.cursor() as cur:
                    # 보통 재고/폭주 판단은 "결제 완료(PAID)" 시점에서만 하는 게 자연스러움
                    if order.get("current_status") == "PAID":
                        is_burst = check_burst_anomaly(order)
                        is_stockout = check_stock_anomaly(cur, order)

                        if is_stockout:
                            final_status = "HOLD"
                            hold_reason = REASON_OOS
                        elif is_burst:
                            final_status = "HOLD"
                            hold_reason = REASON_PROD_FRAUD

                    save_to_db(cur, order, final_status, hold_reason, kafka_offset=message.offset)

                    conn.commit()
                    consumer.commit()

                if final_status == "HOLD":
                    print(f"🛑 [HOLD] {order.get('product_name')} | {order.get('product_id')} | 사유: {hold_reason}")
                else:
                    print(f"✅ [PASS] {final_status} | {order.get('order_id')} | {order.get('product_name')}")

            except Exception as e:
                conn.rollback()
                print(f"🔥 [DB Error] offset={message.offset} err={e}")
                continue

    except KeyboardInterrupt:
        print("\n🛑 anomaly_consumer 종료")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        print("✅ DB / Consumer 정상 종료")


if __name__ == "__main__":
    main()