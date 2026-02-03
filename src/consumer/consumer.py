"""
consumer.py
- Kafka 'event' 토픽에서 주문 이벤트를 수신하여 Postgres에 적재
- ✅ Producer(src/producer/data_factory.py)가 보내는 스키마에 맞춰 consumer가 처리하도록 정리

────────────────────────────────────────────────────────────────────────────
✅ 이번 수정에서 해결한 것 (너가 겪은 에러 기준)
1) orders.user_id NOT NULL 제약 위반 해결
   - producer는 user_id라는 키가 아니라 customer_id를 보냄
   - 그래서 orders.user_id = event["customer_id"] 로 매핑해야 함
   - 이게 빠져있으면 "모든 이벤트"가 orders upsert에서 다 터짐

2) producer 주소 키 호환
   - producer는 address 키를 보냄
   - 기존 consumer가 shipping_address만 보면 주소가 다 null로 들어감
   - shipping_address = event.get("shipping_address") or event.get("address")

3) events(원장)와 orders(스냅샷) 분리 보호
   - orders upsert가 실패해도 events는 저장되도록 SAVEPOINT 사용

4) product_id NOT NULL 제약 대응
   - HOLD 같은 운영 이벤트에서 product_id가 빠질 수 있음
   - (a) 기존 orders에서 product_id/product_name 보강 시도
   - (b) 그래도 없으면 orders upsert만 스킵하고 events는 저장
────────────────────────────────────────────────────────────────────────────
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone, date

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


# =============================================================================
# 환경변수 (docker-compose 기준으로 맞춰 쓰는 걸 추천)
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")
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
def now_utc() -> datetime:
    """UTC now (timezone-aware)"""
    return datetime.now(timezone.utc)


def parse_iso_datetime(value) -> datetime:
    """
    Producer는 last_occurred_at / updated_at 을 ISO 문자열로 보냄.
    예) "2026-02-03T07:04:13.770653" (tz 없는 경우도 가능)
        "2026-02-03T07:04:13.770653+00:00"
        "2026-02-03T07:04:13Z"

    - 파싱 실패 시 now_utc()로 fallback
    """
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


def parse_date(value):
    """
    Producer promised_delivery_date는 "YYYY-MM-DD" 문자열.
    DB는 date 타입이므로 date로 변환.
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
    text 컬럼에 dict/list가 들어오면 오류날 수 있음.
    - dict/list -> JSON 문자열
    - 기타 -> str
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def stable_event_id(order_id: str, last_event_type: str, occurred_at: datetime) -> str:
    """
    Producer는 event_id를 보내지 않음.
    Consumer에서 재시작/재처리 시 중복 insert 줄이기 위해 결정적 UUID 생성.
    """
    if not order_id:
        return str(uuid.uuid4())

    occurred_iso = occurred_at.isoformat() if isinstance(occurred_at, datetime) else str(occurred_at)
    base = f"{order_id}|{last_event_type}|{occurred_iso}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


# =============================================================================
# DB 연결 (재시도)
# =============================================================================
def connect_db_with_retry():
    """Postgres 연결될 때까지 재시도"""
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
# events: 이벤트 원장
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

# orders: 현재 주문 상태 스냅샷
# ✅ 중요: orders.user_id NOT NULL 이라 무조건 넣어야 함
SQL_UPSERT_ORDERS = """
INSERT INTO public.orders (
  order_id,
  user_id,
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
  %(user_id)s,
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
  user_id = EXCLUDED.user_id,
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

# orders: product_id/product_name 보강용 조회
SQL_SELECT_PRODUCT_FROM_ORDERS = """
SELECT user_id, product_id, product_name
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

    # ✅ enable_auto_commit=False : DB commit 성공 후에만 Kafka offset commit
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        for msg in consumer:
            event = msg.value if isinstance(msg.value, dict) else {}

            # -----------------------------------------------------------------
            # (A) Producer 스키마에 맞춰 필드 추출
            # -----------------------------------------------------------------
            order_id = event.get("order_id")

            # ✅ Producer는 customer_id를 보냄 → DB orders.user_id로 매핑
            user_id = event.get("user_id") or event.get("customer_id")

            # 상태 필드 (producer가 보냄)
            current_stage = event.get("current_stage")
            current_status = event.get("current_status")
            last_event_type = event.get("last_event_type") or event.get("event_type") or current_status or "UNKNOWN"

            # 시간 필드 (producer: last_occurred_at, updated_at)
            occurred_at = parse_iso_datetime(event.get("last_occurred_at") or event.get("occurred_at"))
            updated_at = parse_iso_datetime(event.get("updated_at"))  # producer가 주면 사용
            ingested_at = now_utc()

            # 상품 필드 (orders.product_id NOT NULL)
            product_id = event.get("product_id")
            product_name = event.get("product_name")

            # 기타 필드
            tracking_no = event.get("tracking_no")
            promised_delivery_date = parse_date(event.get("promised_delivery_date"))

            # HOLD 관련 (producer는 기본 None)
            hold_reason_code = event.get("hold_reason_code") or event.get("reason_code")

            # ✅ Producer는 주소를 "address"로 보냄
            shipping_address = to_text_or_json(event.get("shipping_address") or event.get("address"))

            # producer는 event_id를 안 주므로 안정적으로 생성
            event_id = event.get("event_id") or stable_event_id(order_id, last_event_type, occurred_at)

            # 콘솔 로그 (디버깅용)
            print("✅ 메시지 수신")
            print(f"   order_id        : {order_id}")
            print(f"   current_status  : {current_status}")
            print(f"   last_event_type : {last_event_type}")
            print(f"   partition       : {msg.partition}")
            print(f"   offset          : {msg.offset}")
            print()

            # -----------------------------------------------------------------
            # (B) payload_json: 원본 이벤트 보존 + 우리가 만든 보강값 추가
            # -----------------------------------------------------------------
            payload_for_db = dict(event)
            payload_for_db["event_id"] = event_id
            payload_for_db["occurred_at"] = occurred_at.isoformat()
            payload_for_db["ingested_at"] = ingested_at.isoformat()

            # -----------------------------------------------------------------
            # (C) DB 적재 정책
            #     - events는 원장: 가능한 한 항상 저장
            #     - orders는 스냅샷: 필수값 없거나 제약 위반이면 스킵/보강
            #
            # ✅ 핵심: SAVEPOINT로 orders만 롤백하여 events는 살린다
            # -----------------------------------------------------------------
            try:
                # 1) events insert (원장)
                cur.execute(
                    SQL_INSERT_EVENTS,
                    {
                        "event_id": event_id,
                        "order_id": order_id,
                        "event_type": last_event_type,
                        "reason_code": hold_reason_code,
                        "occurred_at": occurred_at,
                        "ingested_at": ingested_at,
                        "source": "kafka-producer",
                        "payload_json": Json(payload_for_db),
                    },
                )

                # 2) orders upsert (스냅샷)
                #    - DB not null 대응: user_id, product_id 없으면 보강 시도 후 스킵
                cur.execute("SAVEPOINT sp_orders;")
                try:
                    # 2-1) 최소 필수값 체크 (order_id / stage / status)
                    missing = []
                    if not order_id:
                        missing.append("order_id")
                    if not current_stage:
                        missing.append("current_stage")
                    if not current_status:
                        missing.append("current_status")

                    # 2-2) HOLD 같은 운영 이벤트에서 user_id/product_id가 빠질 수 있어 보강
                    # - 기존 orders에 해당 주문이 이미 있으면 거기서 user_id, product_id, product_name을 가져올 수 있음
                    if order_id and (not user_id or not product_id):
                        cur.execute(SQL_SELECT_PRODUCT_FROM_ORDERS, (order_id,))
                        row = cur.fetchone()
                        if row:
                            existing_user_id, existing_product_id, existing_product_name = row
                            user_id = user_id or existing_user_id
                            product_id = product_id or existing_product_id
                            product_name = product_name or existing_product_name

                    # 2-3) DB NOT NULL 대응: user_id / product_id는 필수
                    if not user_id:
                        missing.append("user_id")
                    if not product_id:
                        missing.append("product_id")

                    # 2-4) 누락이면 orders upsert는 스킵 (events는 이미 들어감)
                    if missing:
                        print(f"⚠️ [SKIP orders] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
                    else:
                        cur.execute(
                            SQL_UPSERT_ORDERS,
                            {
                                "order_id": order_id,
                                "user_id": user_id,  # ✅ 추가/핵심
                                "product_id": product_id,
                                "product_name": product_name,
                                "current_stage": current_stage,
                                "current_status": current_status,
                                "hold_reason_code": hold_reason_code,
                                "last_event_type": last_event_type,
                                "last_occurred_at": occurred_at,
                                "tracking_no": tracking_no,
                                "promised_delivery_date": promised_delivery_date,
                                "shipping_address": shipping_address,
                                # producer updated_at이 있으면 그걸 우선 (없으면 ingested_at)
                                "updated_at": updated_at or ingested_at,
                            },
                        )

                except Exception as e_orders:
                    # orders만 롤백하고 events는 살린다
                    cur.execute("ROLLBACK TO SAVEPOINT sp_orders;")
                    print(f"⚠️ [orders upsert 실패 - events는 저장됨] event_id={event_id} err={e_orders}")

                # 3) 최종 커밋: events는 이미 들어갔고 orders는 성공했으면 같이 들어감
                conn.commit()

                # 4) Kafka offset commit: DB 커밋 성공 후에만!
                consumer.commit()

            except Exception as e:
                # events insert 자체가 실패한 경우 (DB 연결 문제/스키마 문제 등)
                conn.rollback()
                print(f"❌ [DB 처리 실패] event_id={event_id} order_id={order_id} error={e}")
                # 오프셋 커밋 안 함 → 재처리로 유실 방지
                continue

    except KeyboardInterrupt:
        print("\n🛑 Consumer 종료")
    finally:
        # 자원 정리
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