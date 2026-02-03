import json
import os
import time
import uuid
from datetime import datetime, timezone, date

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


# =============================================================================
# 환경변수
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")  # ★ producer랑 반드시 동일
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-reader")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.239.40")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")


# =============================================================================
# 유틸: 시간/날짜 파싱
# =============================================================================
def now_utc():
    return datetime.now(timezone.utc)


def parse_occurred_at(value):
    """
    producer가 occurred_at을
    - ISO 문자열("2026-02-02T07:37:35Z" 등)로 주거나
    - 아예 안 주거나
    - 이상한 값으로 줄 수 있어서 방어
    """
    if not value:
        return now_utc()

    # 이미 datetime이면 그대로
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    # 문자열이면 ISO 파싱 시도
    if isinstance(value, str):
        v = value.strip()
        try:
            # "Z" 처리
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return now_utc()

    # 그 외 타입이면 now
    return now_utc()


def parse_date(value):
    """
    promised_delivery_date 방어 파서 (DB는 date 타입)
    - "YYYY-MM-DD" -> date
    - datetime/date -> date
    - 이상하면 None
    """
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        v = value.strip()
        try:
            return date.fromisoformat(v)
        except Exception:
            return None

    return None


def to_text_or_json(value):
    """
    shipping_address 같은 값이 dict로 오면 그대로 넣으면 PG text 컬럼에서 터질 수 있음.
    - dict/list -> JSON 문자열로 변환
    - 그 외 -> 그대로
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def stable_event_id(order_id, last_event_type, occurred_at):
    """
    event_id가 producer에서 안 올 때:
    - 재처리/재시작 시 중복 적재를 줄이기 위해 결정적(Deterministic) ID 생성
    - order_id가 없으면 안정적으로 만들 수 없으니 uuid4로 fallback
    """
    if not order_id:
        return str(uuid.uuid4())

    # occurred_at은 가능한 producer의 last_occurred_at(or occurred_at) 기반으로 안정적으로
    occurred_iso = occurred_at.isoformat() if isinstance(occurred_at, datetime) else str(occurred_at)

    base = f"{order_id}|{last_event_type}|{occurred_iso}"
    # DNS 네임스페이스 사용(아무거나 상관없음). 같은 base면 같은 uuid.
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
# SQL
# =============================================================================
# events: 이벤트 원장 (shipping_address 컬럼 없음!)
SQL_INSERT_EVENTS = """
INSERT INTO public.events (
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  ingested_at,
  source,
  payload_json
) VALUES (
  %(event_id)s,
  %(order_id)s,
  %(event_type)s,
  %(reason_code)s,
  %(occurred_at)s,
  %(ingested_at)s,
  %(source)s,
  %(payload_json)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

# orders: 현재 주문 상태 스냅샷 (shipping_address는 여기!)
SQL_UPSERT_ORDERS = """
INSERT INTO public.orders (
  order_id,
  product_id,
  product_name,
  current_stage,
  current_status,
  hold_reason_code,
  last_event_type,
  last_occurred_at,
  tracking_no,
  promised_delivery_date,
  shipping_address,
  updated_at
) VALUES (
  %(order_id)s,
  %(product_id)s,
  %(product_name)s,
  %(current_stage)s,
  %(current_status)s,
  %(hold_reason_code)s,
  %(last_event_type)s,
  %(last_occurred_at)s,
  %(tracking_no)s,
  %(promised_delivery_date)s,
  %(shipping_address)s,
  %(updated_at)s
)
ON CONFLICT (order_id)
DO UPDATE SET
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
  updated_at = EXCLUDED.updated_at;
"""


# =============================================================================
# 메인
# =============================================================================
def main():
    print("📨 Kafka Consumer 시작")
    print("=" * 60)

    # Kafka Consumer (kafka-python)
    # ✅ enable_auto_commit=False : DB에 성공적으로 적재한 뒤에만 오프셋 커밋 (유실 방지)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        # producer가 utf-8 JSON으로 보내니까 그대로 dict로
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        for msg in consumer:
            event = msg.value if isinstance(msg.value, dict) else {}

            # -----------------------------
            # (A) 최소 보정/정규화
            # -----------------------------
            order_id = event.get("order_id")
            current_stage = event.get("current_stage")
            current_status = event.get("current_status")

            hold_reason_code = event.get("hold_reason_code") or event.get("reason_code")

            # ✅ producer는 last_occurred_at를 주는 구조라서 호환 처리
            occurred_at = parse_occurred_at(
                event.get("occurred_at") or event.get("last_occurred_at")
            )
            ingested_at = now_utc()

            product_id = event.get("product_id")
            product_name = event.get("product_name")

            tracking_no = event.get("tracking_no")
            promised_delivery_date = parse_date(event.get("promised_delivery_date"))

            # orders에 들어갈 주소 (events 테이블 컬럼에는 없음!)
            shipping_address = to_text_or_json(event.get("shipping_address"))

            # ✅ 핵심 규칙:
            # events.event_type 은 "orders에 들어갈 last_event_type" 값을 따라가야 함
            # 즉 orders.last_event_type -> events.event_type
            last_event_type = (
                event.get("last_event_type")
                or event.get("event_type")
                or current_status
                or "UNKNOWN"
            )

            # event_id 없으면 안정적으로 생성 (재처리 중복 방지)
            event_id = event.get("event_id") or stable_event_id(order_id, last_event_type, occurred_at)

            # 콘솔 로그
            print("✅ 메시지 수신")
            print(f"   order_id        : {order_id}")
            print(f"   current_status  : {current_status}")
            print(f"   last_event_type : {last_event_type}")
            print(f"   partition       : {msg.partition}")
            print(f"   offset          : {msg.offset}")
            print()

            # payload_json: 원본 이벤트를 최대한 보존 (디버깅에 유리)
            payload_for_db = dict(event)
            payload_for_db["event_id"] = event_id
            payload_for_db["occurred_at"] = occurred_at.isoformat()
            # producer가 주는 필드도 같이 보존 (있으면)
            if "last_occurred_at" in event:
                payload_for_db["last_occurred_at"] = str(event.get("last_occurred_at"))

            # -----------------------------
            # (B) events + orders를 "가능하면" 한 트랜잭션으로 처리
            #     - events는 무조건 저장 시도
            #     - orders는 필수값이 있을 때만 UPSERT
            #     - 성공적으로 DB commit 된 뒤에만 consumer.commit()
            # -----------------------------
            try:
                # 1) events insert
                cur.execute(SQL_INSERT_EVENTS, {
                    "event_id": event_id,
                    "order_id": order_id,
                    "event_type": last_event_type,   # ✅ 여기!
                    "reason_code": hold_reason_code,
                    "occurred_at": occurred_at,
                    "ingested_at": ingested_at,
                    "source": "kafka-producer",
                    "payload_json": Json(payload_for_db),
                })

                # 2) orders upsert (필수값 체크)
                missing = []
                if not order_id:
                    missing.append("order_id")
                if not current_stage:
                    missing.append("current_stage")
                if not current_status:
                    missing.append("current_status")

                if missing:
                    # events는 저장했지만 orders는 스킵 (정책상 OK)
                    print(f"⚠️ [SKIP orders] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
                else:
                    cur.execute(SQL_UPSERT_ORDERS, {
                        "order_id": order_id,
                        "product_id": product_id,
                        "product_name": product_name,
                        "current_stage": current_stage,
                        "current_status": current_status,
                        "hold_reason_code": hold_reason_code,
                        "last_event_type": last_event_type,  # ✅ orders.last_event_type
                        "last_occurred_at": occurred_at,
                        "tracking_no": tracking_no,
                        "promised_delivery_date": promised_delivery_date,
                        "shipping_address": shipping_address,
                        "updated_at": ingested_at,
                    })

                # DB 커밋이 성공해야만 오프셋 커밋
                conn.commit()
                consumer.commit()

            except Exception as e:
                conn.rollback()
                print(f"❌ [DB 처리 실패] event_id={event_id} order_id={order_id} error={e}")
                # 오프셋 커밋을 안 했으니, 같은 메시지가 재처리됨 (유실 방지)
                continue

    except KeyboardInterrupt:
        print("\n🛑 Consumer 종료")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        consumer.close()
        print("✅ DB / Consumer 정상 종료")


if __name__ == "__main__":
    main()