import json
import os
import time
import uuid
from datetime import datetime, timezone

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
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "2.0"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")


# =============================================================================
# 유틸: 시간 파싱
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
SQL_INSERT_EVENTS = """
INSERT INTO events (
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  source,
  payload_json
) VALUES (
  %(event_id)s,
  %(order_id)s,
  %(event_type)s,
  %(reason_code)s,
  %(occurred_at)s,
  %(source)s,
  %(payload_json)s
)
ON CONFLICT (event_id) DO NOTHING;
"""

SQL_UPSERT_ORDER_CURRENT = """
INSERT INTO order_current (
  order_id,
  current_stage,
  current_status,
  hold_reason_code,
  last_event_type,
  last_occurred_at,
  tracking_no,
  promised_delivery_date,
  updated_at
) VALUES (
  %(order_id)s,
  %(current_stage)s,
  %(current_status)s,
  %(hold_reason_code)s,
  %(last_event_type)s,
  %(last_occurred_at)s,
  %(tracking_no)s,
  %(promised_delivery_date)s,
  now()
)
ON CONFLICT (order_id)
DO UPDATE SET
  current_stage = EXCLUDED.current_stage,
  current_status = EXCLUDED.current_status,
  hold_reason_code = EXCLUDED.hold_reason_code,
  last_event_type = EXCLUDED.last_event_type,
  last_occurred_at = EXCLUDED.last_occurred_at,
  tracking_no = EXCLUDED.tracking_no,
  promised_delivery_date = EXCLUDED.promised_delivery_date,
  updated_at = now();
"""


# =============================================================================
# 메인
# =============================================================================
def main():
    print("📨 Kafka Consumer 시작")
    print("=" * 60)

    # Kafka Consumer (kafka-python)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
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
            # event_id 없으면 생성 (PK)
            event_id = event.get("event_id") or str(uuid.uuid4())

            order_id = event.get("order_id")
            current_stage = event.get("current_stage")
            current_status = event.get("current_status")

            # event_type 없으면 status로 대체 (NOT NULL 방어)
            event_type = event.get("event_type") or current_status or "UNKNOWN"

            hold_reason_code = event.get("hold_reason_code")
            occurred_at = parse_occurred_at(event.get("occurred_at"))

            tracking_no = event.get("tracking_no")
            promised_delivery_date = event.get("promised_delivery_date")  # 문자열이어도 PG가 date cast 가능하면 처리됨

            # 콘솔 로그 (수업 형태 유지)
            print("✅ 메시지 수신")
            print(f"   order_id : {order_id}")
            print(f"   status   : {current_status}")
            print(f"   partition: {msg.partition}")
            print(f"   offset   : {msg.offset}")
            print()

            # payload는 "원본 + 보정한 일부"까지 포함해서 남겨도 되고,
            # 여기선 원본 event 그대로 저장 + event_id/occurred_at 보정값을 덮어씌워 저장(추천)
            payload_for_db = dict(event)
            payload_for_db["event_id"] = event_id
            # occurred_at은 datetime을 json으로 못 넣으니 ISO 문자열로 넣어줌
            payload_for_db["occurred_at"] = occurred_at.isoformat()

            # -----------------------------
            # (B) 1) events는 무조건 저장
            # -----------------------------
            try:
                cur.execute(SQL_INSERT_EVENTS, {
                    "event_id": event_id,
                    "order_id": order_id,
                    "event_type": event_type,
                    "reason_code": hold_reason_code,   # 너 테이블 컬럼이 reason_code라 여기에 HOLD 사유 넣음
                    "occurred_at": occurred_at,
                    "source": "kafka-producer",
                    "payload_json": Json(payload_for_db),
                })
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ [events 저장 실패] event_id={event_id} error={e}")
                # events 저장 실패해도 consumer는 계속 돌게 함
                continue

            # -----------------------------
            # (C) 2) order_current는 검증 통과 시만 UPSERT
            # -----------------------------
            missing = []
            if not order_id:
                missing.append("order_id")
            if not current_stage:
                missing.append("current_stage")
            if not current_status:
                missing.append("current_status")

            if missing:
                print(f"⚠️ [SKIP order_current] 필수값 누락: {', '.join(missing)} (event_id={event_id})")
                continue

            try:
                cur.execute(SQL_UPSERT_ORDER_CURRENT, {
                    "order_id": order_id,
                    "current_stage": current_stage,
                    "current_status": current_status,
                    "hold_reason_code": hold_reason_code,
                    "last_event_type": event_type,      # NOT NULL 보장
                    "last_occurred_at": occurred_at,    # NOT NULL 보장
                    "tracking_no": tracking_no,
                    "promised_delivery_date": promised_delivery_date,
                })
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ [order_current 갱신 실패] order_id={order_id} event_id={event_id} error={e}")
                # order_current 실패해도 consumer는 계속 돌게 함
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