"""
consumer.py (DB-sql 현재 구조 기준)

- Kafka 'event' 토픽에서 주문 이벤트를 수신하여 Postgres에 적재
- ✅ 현재 DB 구조: orders_raw(원본) → events(원장) → orders(스냅샷)

────────────────────────────────────────────────────────────────────────────
✅ 현재 DB 구조에 맞춰 변경된 적재 흐름
1) orders_raw에 원본(raw_payload) 저장 후 raw_id 확보
2) events(원장) insert (가능한 한 항상 저장)
3) orders(스냅샷) upsert (실패해도 events는 남게 SAVEPOINT)
────────────────────────────────────────────────────────────────────────────
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


# =============================================================================
# 환경변수 (docker-compose 기준 권장)
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-reader")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

# ✅ 팀 DB 접속 규칙: localhost 사용 안 함 (기본값은 팀 DB IP로)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.239.40")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")


# =============================================================================
# 유틸: 시간 파싱
# =============================================================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value) -> datetime:
    if not value:
        return now_utc()

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        v = value.strip()
        try:
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return now_utc()

    return now_utc()


def to_text_or_json(value):
    """
    shipping_address(text)에 dict/list가 들어오면 오류 가능
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
    """
    if not order_id:
        return str(uuid.uuid4())
    base = f"{order_id}|{event_type}|{occurred_at.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


# =============================================================================
# DB 연결 (재시도)
# =============================================================================
def connect_db_with_retry():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            print("✅ Postgres 연결 성공")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# =============================================================================
# SQL (현재 DB-sql 구조 기준)
# =============================================================================

# 1) orders_raw: 원본 저장 (raw_id 받아오기)
SQL_INSERT_ORDERS_RAW = """
INSERT INTO public.orders_raw (
  raw_payload,
  kafka_offset,
  ingested_at
) VALUES (
  %(raw_payload)s,
  %(kafka_offset)s,
  %(ingested_at)s
)
RETURNING raw_id;
"""

# 2) events: 이벤트 원장
# DB 컬럼:
#   event_id, order_id, event_type, current_status, reason_code,
#   occurred_at, ingested_at,
#   ops_status, ops_note, ops_operator, ops_updated_at
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
  %(event_id)s,
  %(order_id)s,
  %(event_type)s,
  %(current_status)s,
  %(reason_code)s,
  %(occurred_at)s,
  %(ingested_at)s,
  %(ops_status)s,
  %(ops_note)s,
  %(ops_operator)s,
  %(ops_updated_at)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

# 3) orders: 스냅샷
# DB 컬럼:
#   order_id, user_id, product_id, product_name, shipping_address,
#   current_stage, current_status, last_event_type, last_occurred_at,
#   hold_reason_code,
#   hold_ops_status, hold_ops_note, hold_ops_operator, hold_ops_updated_at,
#   raw_reference_id, created_at(default now())
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
  %(order_id)s,
  %(user_id)s,
  %(product_id)s,
  %(product_name)s,
  %(shipping_address)s,
  %(current_stage)s,
  %(current_status)s,
  %(last_event_type)s,
  %(last_occurred_at)s,
  %(hold_reason_code)s,
  %(hold_ops_status)s,
  %(hold_ops_note)s,
  %(hold_ops_operator)s,
  %(hold_ops_updated_at)s,
  %(raw_reference_id)s
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

# orders: HOLD 등에서 user_id/product_id/product_name 보강용 조회
SQL_SELECT_FROM_ORDERS = """
SELECT user_id, product_id, product_name, shipping_address
FROM public.orders
WHERE order_id = %s
LIMIT 1;
"""


# =============================================================================
# 메인
# =============================================================================
def main():
    print("📨 Kafka Consumer 시작")
    print("=" * 60)
    print(f"- topic      : {KAFKA_TOPIC}")
    print(f"- bootstrap  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"- group_id   : {KAFKA_GROUP_ID}")
    print(f"- offset     : {AUTO_OFFSET_RESET}")
    print("=" * 60)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,  # ✅ DB 커밋 성공 후에만 offset commit
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        for msg in consumer:
            event = msg.value if isinstance(msg.value, dict) else {}

            # -------------------------------------------------------------
            # (A) Producer 스키마 기반 필드 추출/정규화
            # -------------------------------------------------------------
            order_id = event.get("order_id")

            # producer: customer_id → DB: user_id
            user_id = event.get("user_id") or event.get("customer_id")

            current_stage = event.get("current_stage")
            current_status = event.get("current_status")

            # event_type 우선순위:
            #  - last_event_type(스냅샷용) -> event_type -> current_status -> UNKNOWN
            event_type = (
                event.get("last_event_type")
                or event.get("event_type")
                or current_status
                or "UNKNOWN"
            )

            occurred_at = parse_iso_datetime(
                event.get("last_occurred_at") or event.get("occurred_at")
            )
            ingested_at = now_utc()

            product_id = event.get("product_id")
            product_name = event.get("product_name")

            # 주소 (orders.shipping_address는 NOT NULL)
            shipping_address = to_text_or_json(event.get("shipping_address") or event.get("address"))

            # reason_code (events.reason_code / orders.hold_reason_code)
            reason_code = event.get("reason_code") or event.get("hold_reason_code")

            # ── ops 컬럼명 변경 반영 ─────────────────────────────────────────
            # events: ops_status, ops_note, ops_operator, ops_updated_at
            ops_status = event.get("ops_status")
            ops_note = event.get("ops_note") or event.get("ops_comment")
            ops_operator = event.get("ops_operator") or event.get("ops_user")
            ops_updated_at = parse_iso_datetime(event.get("ops_updated_at")) if event.get("ops_updated_at") else None

            # orders: hold_ops_status, hold_ops_note, hold_ops_operator, hold_ops_updated_at
            hold_ops_status = event.get("hold_ops_status")
            hold_ops_note = event.get("hold_ops_note") or event.get("hold_ops_comment")
            hold_ops_operator = event.get("hold_ops_operator") or event.get("hold_ops_user")
            hold_ops_updated_at = (
                parse_iso_datetime(event.get("hold_ops_updated_at"))
                if event.get("hold_ops_updated_at")
                else None
            )
            # ─────────────────────────────────────────────────────────────

            event_id = event.get("event_id") or stable_event_id(order_id, event_type, occurred_at)

            print("✅ 메시지 수신")
            print(f"   order_id        : {order_id}")
            print(f"   event_type      : {event_type}")
            print(f"   current_status  : {current_status}")
            print(f"   partition       : {msg.partition}")
            print(f"   offset          : {msg.offset}")
            print()

            # -------------------------------------------------------------
            # (B) DB 적재 정책
            # 1) orders_raw 저장 + raw_id 확보
            # 2) events insert (원장)
            # 3) orders upsert (스냅샷) - 실패해도 events/raw는 살린다
            # -------------------------------------------------------------
            try:
                # 0) 원본 payload에 메타 추가(선택)
                payload_for_raw = dict(event)
                payload_for_raw["_meta"] = {
                    "kafka_topic": msg.topic,
                    "kafka_partition": msg.partition,
                    "kafka_offset": msg.offset,
                    "ingested_at": ingested_at.isoformat(),
                    "derived_event_id": event_id,
                }

                # 1) orders_raw insert → raw_id
                cur.execute(
                    SQL_INSERT_ORDERS_RAW,
                    {
                        "raw_payload": Json(payload_for_raw),
                        "kafka_offset": msg.offset,
                        "ingested_at": ingested_at,
                    },
                )
                raw_id = cur.fetchone()[0]

                # 2) events insert (원장)
                #    current_status가 NOT NULL이므로 fallback 보장
                cur.execute(
                    SQL_INSERT_EVENTS,
                    {
                        "event_id": event_id,
                        "order_id": order_id,
                        "event_type": event_type or "UNKNOWN",
                        "current_status": current_status or event_type or "UNKNOWN",
                        "reason_code": reason_code,
                        "occurred_at": occurred_at,
                        "ingested_at": ingested_at,
                        "ops_status": ops_status,
                        "ops_note": ops_note,
                        "ops_operator": ops_operator,
                        "ops_updated_at": ops_updated_at,
                    },
                )

                # 3) orders upsert (스냅샷) - SAVEPOINT
                cur.execute("SAVEPOINT sp_orders;")
                try:
                    missing = []

                    # orders PK/NOT NULL 필드들
                    if not order_id:
                        missing.append("order_id")
                    if not current_stage:
                        missing.append("current_stage")
                    if not current_status:
                        missing.append("current_status")

                    # HOLD 같은 이벤트에서 user_id/product_id/product_name/address가 빠질 수 있으니 보강 시도
                    if order_id and (not user_id or not product_id or not product_name or not shipping_address):
                        cur.execute(SQL_SELECT_FROM_ORDERS, (order_id,))
                        row = cur.fetchone()
                        if row:
                            existing_user_id, existing_product_id, existing_product_name, existing_shipping_address = row
                            user_id = user_id or existing_user_id
                            product_id = product_id or existing_product_id
                            product_name = product_name or existing_product_name
                            shipping_address = shipping_address or existing_shipping_address

                    # 최종 NOT NULL 검증
                    if not user_id:
                        missing.append("user_id")
                    if not product_id:
                        missing.append("product_id")
                    if not product_name:
                        missing.append("product_name")
                    if not shipping_address:
                        missing.append("shipping_address")

                    if missing:
                        print(f"⚠️ [SKIP orders] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
                    else:
                        cur.execute(
                            SQL_UPSERT_ORDERS,
                            {
                                "order_id": order_id,
                                "user_id": user_id,
                                "product_id": product_id,
                                "product_name": product_name,
                                "shipping_address": shipping_address,
                                "current_stage": current_stage,
                                "current_status": current_status,
                                "last_event_type": event_type,
                                "last_occurred_at": occurred_at,
                                "hold_reason_code": reason_code,
                                "hold_ops_status": hold_ops_status,
                                "hold_ops_note": hold_ops_note,
                                "hold_ops_operator": hold_ops_operator,
                                "hold_ops_updated_at": hold_ops_updated_at,
                                "raw_reference_id": raw_id,  # ✅ NOT NULL + FK 만족
                            },
                        )

                except Exception as e_orders:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
                    print(f"⚠️ [orders upsert 실패 - raw/events는 저장됨] event_id={event_id} err={e_orders}")

                # 4) 커밋 후 offset 커밋
                conn.commit()
                consumer.commit()

            except Exception as e:
                conn.rollback()
                print(f"❌ [DB 처리 실패] event_id={event_id} order_id={order_id} error={e}")
                # offset commit 안 함 → 재처리로 유실 방지
                continue

    except KeyboardInterrupt:
        print("\n🛑 Consumer 종료")
    finally:
        try:
            cur.close()
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