"""
✅ Kafka 'event' 토픽에서 주문 이벤트를 수신하여 Postgres에 적재 (배치/트랜잭션 기반)

[핵심 정정 사항 - 스키마 기준]
- orders_raw = "이상 데이터 전용"이 아니라, ✅ 모든 원본(raw_payload)을 항상 저장하는 테이블
- orders      = 정상 데이터(필수값 충족)만 스냅샷 upsert, ✅ raw_reference_id 반드시 포함(orders_raw.raw_id FK)
- events      = 가능한 한 항상 저장(원장)
- 이상 데이터(user_id or shipping_address 누락 등)는 ✅ 별도 테이블(예: orders_invalid)에 저장

[처리 순서(트랜잭션 1번)]
1) orders_raw  : 배치 전체 원본을 먼저 벌크 insert + RETURNING raw_id 로 raw_id 확보
2) events      : 항상 insert (ON CONFLICT DO NOTHING)
3) orders      : 정상 데이터만 upsert (raw_reference_id=raw_id 포함)
4) orders_invalid: 이상 데이터 기록 (raw_reference_id=raw_id 포함)
5) DB commit 성공 후에만 Kafka offset commit

※ 참고: orders_invalid 테이블이 DB에 없으면, 아래 DDL로 생성 필요
-----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.orders_invalid (
  invalid_id        BIGSERIAL PRIMARY KEY,
  raw_reference_id  BIGINT NOT NULL REFERENCES public.orders_raw(raw_id),
  event_id          TEXT NULL,
  order_id          TEXT NULL,
  missing_fields    TEXT[] NOT NULL,
  detected_at       TIMESTAMPTZ DEFAULT now(),
  note              TEXT NULL
);
-----------------------------------------------------------------------
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata


# =============================================================================
# 환경변수 (docker-compose 기준 권장)
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-reader")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

# ✅ 배치 설정
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))              # 최대 1000개씩
FLUSH_EVERY_SEC = float(os.getenv("FLUSH_EVERY_SEC", "1.0"))   # 1초마다 flush (덜 차도)

# ✅ 팀 DB 접속 규칙: localhost 사용 안 함 (기본값은 팀 DB IP로)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.239.40")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")


# =============================================================================
# 유틸: 시간/타입 파싱
# =============================================================================
def now_utc() -> datetime:
    """현재 UTC 시간(aware datetime)"""
    return datetime.now(timezone.utc)


def parse_iso_datetime(value) -> datetime:
    """
    - None/빈값이면 현재 UTC
    - str이면 ISO8601 파싱 (Z -> +00:00 처리)
    - datetime이면 tz 없는 경우 UTC로 간주
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


def stable_event_id(
    order_id: Optional[str],
    event_type: str,
    occurred_at: datetime,
    *,
    topic: Optional[str] = None,
    partition: Optional[int] = None,
    offset: Optional[int] = None,
) -> str:
    """
    producer가 event_id를 안 주는 경우에도 "재처리 시" 중복 insert가 덜 생기게
    결정적 UUID(uuid5) 생성

    - order_id가 있으면: (order_id | event_type | occurred_at) 기반
    - order_id가 없으면: (topic | partition | offset | event_type | occurred_at) 기반
    """
    if order_id:
        base = f"{order_id}|{event_type}|{occurred_at.isoformat()}"
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))

    base = f"{topic}|{partition}|{offset}|{event_type}|{occurred_at.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


# =============================================================================
# DB 연결 (재시도)
# =============================================================================
def connect_db_with_retry():
    """DB 연결이 될 때까지 3초 간격으로 재시도"""
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False  # ✅ 트랜잭션 직접 제어
            print("✅ Postgres 연결 성공")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# =============================================================================
# SQL (현재 DB 구조 기준)
# =============================================================================

# 1) orders_raw: ✅ 모든 원본 저장 + raw_id 반환(orders/orders_invalid가 참조해야 함)
SQL_INSERT_ORDERS_RAW_VALUES_RETURNING = """
INSERT INTO public.orders_raw (
  raw_payload,
  kafka_offset,
  ingested_at
) VALUES %s
RETURNING raw_id;
"""

# 2) events: 이벤트 원장 (가능한 한 항상 저장)
SQL_INSERT_EVENTS_VALUES = """
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
) VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

# 3) orders: 스냅샷 upsert (✅ 정상 데이터만 + raw_reference_id 필수)
SQL_UPSERT_ORDERS_VALUES = """
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
) VALUES %s
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

# 4) orders_invalid: 이상 데이터 기록 (✅ 테이블이 DB에 있어야 함)
SQL_INSERT_ORDERS_INVALID_VALUES = """
INSERT INTO public.orders_invalid (
  raw_reference_id,
  event_id,
  order_id,
  missing_fields,
  detected_at,
  note
) VALUES %s;
"""

# 5) 보강 조회: HOLD 등에서 product_id/product_name이 누락될 수 있어 orders에서 보강
SQL_SELECT_FROM_ORDERS = """
SELECT user_id, product_id, product_name, shipping_address
FROM public.orders
WHERE order_id = %s
LIMIT 1;
"""


# =============================================================================
# 배치 로우 구성 함수 (orders_raw는 항상 저장하므로 2단계로 나눔)
# =============================================================================
def build_orders_raw_rows(messages) -> Tuple[List[Tuple], Dict[Tuple[str, int], int]]:
    """
    ✅ 1단계: orders_raw에 넣을 rows를 만든다 (항상 저장)
    - raw_payload에는 원본 event에 _meta를 붙여두면 추적/디버깅이 편해짐
    - offset 커밋을 위해 파티션별 마지막 offset도 함께 계산
    """
    orders_raw_rows: List[Tuple] = []
    last_offsets: Dict[Tuple[str, int], int] = {}  # {(topic, partition): last_offset}

    for msg in messages:
        # Kafka value_deserializer로 이미 dict가 들어오는 걸 기대하지만 방어적으로 처리
        event = msg.value if isinstance(msg.value, dict) else {}

        ingested_at = now_utc()

        payload_for_raw = dict(event)
        payload_for_raw["_meta"] = {
            "kafka_topic": msg.topic,
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "ingested_at": ingested_at.isoformat(),
        }

        # orders_raw: (raw_payload, kafka_offset, ingested_at)
        orders_raw_rows.append((Json(payload_for_raw), msg.offset, ingested_at))

        # offset 커밋 계산용 (파티션별 마지막 offset 갱신)
        last_offsets[(msg.topic, msg.partition)] = msg.offset

    return orders_raw_rows, last_offsets


def build_events_orders_invalid_rows(
    cur,
    messages,
    raw_ids: List[int],
) -> Tuple[List[Tuple], List[Tuple], List[Tuple]]:
    """
    ✅ 2단계: orders_raw에서 RETURNING 받은 raw_ids(메시지와 1:1)를 기반으로
    - events  : 가능한 한 항상 저장
    - orders  : 정상 데이터(user_id, shipping_address, order_id 필수)만 upsert
    - invalid : 이상 데이터는 orders_invalid에 기록

    ※ 같은 배치에서 동일 order_id 보강 조회 최적화를 위해 캐시 사용
    """
    events_rows: List[Tuple] = []
    orders_rows: List[Tuple] = []
    invalid_rows: List[Tuple] = []

    # ✅ 보강 조회 캐시
    orders_cache: Dict[str, Optional[Tuple]] = {}

    # 메시지와 raw_id는 반드시 같은 길이/순서여야 함
    for msg, raw_id in zip(messages, raw_ids):
        event = msg.value if isinstance(msg.value, dict) else {}

        # ---------------------------
        # (A) 필드 추출/정규화
        # ---------------------------
        order_id = event.get("order_id")

        # producer: customer_id → consumer/DB: user_id
        user_id = event.get("user_id") or event.get("customer_id")

        current_stage = event.get("current_stage")
        current_status = event.get("current_status")

        # event_type 우선순위:
        # - last_event_type(스냅샷용) -> event_type -> current_status -> UNKNOWN
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

        shipping_address = to_text_or_json(
            event.get("shipping_address") or event.get("address")
        )

        # reason_code (events.reason_code / orders.hold_reason_code)
        reason_code = event.get("reason_code") or event.get("hold_reason_code")

        # events ops 컬럼
        ops_status = event.get("ops_status")
        ops_note = event.get("ops_note") or event.get("ops_comment")
        ops_operator = event.get("ops_operator") or event.get("ops_user")
        ops_updated_at = (
            parse_iso_datetime(event.get("ops_updated_at"))
            if event.get("ops_updated_at")
            else None
        )

        # orders hold_ops 컬럼
        hold_ops_status = event.get("hold_ops_status")
        hold_ops_note = event.get("hold_ops_note") or event.get("hold_ops_comment")
        hold_ops_operator = event.get("hold_ops_operator") or event.get("hold_ops_user")
        hold_ops_updated_at = (
            parse_iso_datetime(event.get("hold_ops_updated_at"))
            if event.get("hold_ops_updated_at")
            else None
        )

        # event_id: producer 제공 시 사용, 없으면 결정적 생성
        event_id = event.get("event_id") or stable_event_id(
            order_id,
            event_type,
            occurred_at,
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
        )

        # ---------------------------
        # (B) events는 가능한 한 항상 저장
        # ---------------------------
        events_rows.append(
            (
                event_id,
                order_id,
                event_type or "UNKNOWN",
                current_status or event_type or "UNKNOWN",
                reason_code,
                occurred_at,
                ingested_at,
                ops_status,
                ops_note,
                ops_operator,
                ops_updated_at,
            )
        )

        # ---------------------------
        # (C) 정상/이상 판정
        # ---------------------------
        missing_fields = []

        if not order_id:
            missing_fields.append("order_id")
        if not user_id:
            missing_fields.append("user_id")
        if not shipping_address:
            missing_fields.append("shipping_address")

        is_invalid = len(missing_fields) > 0

        # ---------------------------
        # (D) 이상 데이터 → orders_invalid 기록
        # ---------------------------
        if is_invalid:
            invalid_rows.append(
                (
                    raw_id,            # raw_reference_id (FK)
                    event_id,          # event_id (추적용)
                    order_id,          # order_id (없을 수도)
                    missing_fields,    # TEXT[]로 저장
                    ingested_at,       # detected_at
                    "Missing required fields for orders snapshot",  # note
                )
            )
            continue

        # ---------------------------
        # (E) 정상 데이터 → orders upsert
        #     (HOLD 등에서 product_id/product_name 누락 시 orders에서 보강)
        # ---------------------------
        if order_id and (not product_id or not product_name):
            if order_id in orders_cache:
                cached = orders_cache[order_id]
            else:
                cur.execute(SQL_SELECT_FROM_ORDERS, (order_id,))
                cached = cur.fetchone()
                orders_cache[order_id] = cached

            if cached:
                existing_user_id, existing_product_id, existing_product_name, existing_shipping_address = cached
                # 안전 보강
                user_id = user_id or existing_user_id
                shipping_address = shipping_address or existing_shipping_address
                product_id = product_id or existing_product_id
                product_name = product_name or existing_product_name

        # 보강 후에도 필수값이 비면(드물지만) invalid로 넘기는 게 안전
        missing_after_enrich = []
        if not order_id:
            missing_after_enrich.append("order_id")
        if not user_id:
            missing_after_enrich.append("user_id")
        if not shipping_address:
            missing_after_enrich.append("shipping_address")

        if missing_after_enrich:
            invalid_rows.append(
                (
                    raw_id,
                    event_id,
                    order_id,
                    missing_after_enrich,
                    ingested_at,
                    "Missing required fields after enrichment attempt",
                )
            )
            continue

        orders_rows.append(
            (
                order_id,
                user_id,
                product_id,
                product_name,
                shipping_address,
                current_stage,
                current_status,
                event_type,
                occurred_at,
                reason_code,
                hold_ops_status,
                hold_ops_note,
                hold_ops_operator,
                hold_ops_updated_at,
                raw_id,  # ✅ raw_reference_id 반드시 포함
            )
        )

    return events_rows, orders_rows, invalid_rows


# =============================================================================
# Kafka offset commit (정확히)
# =============================================================================
def commit_offsets_exactly(consumer, last_offsets: Dict[Tuple[str, int], int]):
    """
    ✅ 배치에서 처리한 마지막 offset 기준으로 정확히 커밋
    - Kafka commit은 "다음에 읽을 offset"을 넣어야 하므로 last_offset+1
    """
    if not last_offsets:
        return

    offsets = {}
    for (topic, partition), last_offset in last_offsets.items():
        tp = TopicPartition(topic, partition)
        offsets[tp] = OffsetAndMetadata(last_offset + 1, None)

    consumer.commit(offsets=offsets)


# =============================================================================
# 배치 처리 (트랜잭션 1번)
# =============================================================================
def process_batch(conn, consumer, messages):
    """
    - messages: Kafka 메시지 리스트
    - 트랜잭션 1번에 orders_raw/events/orders/orders_invalid를 벌크 처리
    - 성공하면 정확한 offset commit
    """
    if not messages:
        return

    cur = conn.cursor()

    try:
        # -------------------------------------------------------------
        # 1) orders_raw: ✅ 항상 저장 + raw_id 반환
        # -------------------------------------------------------------
        orders_raw_rows, last_offsets = build_orders_raw_rows(messages)

        # execute_values로 bulk insert + RETURNING raw_id
        # - psycopg2에서는 RETURNING이 있을 때 cur.fetchall()로 결과를 받을 수 있음
        execute_values(
            cur,
            SQL_INSERT_ORDERS_RAW_VALUES_RETURNING,
            orders_raw_rows,
            page_size=min(BATCH_SIZE, 1000),
        )
        returned = cur.fetchall()
        raw_ids = [row[0] for row in returned]  # [(raw_id,), ...] -> [raw_id, ...]

        # 안전장치: 길이 매칭 확인
        if len(raw_ids) != len(messages):
            raise RuntimeError(
                f"orders_raw RETURNING raw_id count mismatch: raw_ids={len(raw_ids)} messages={len(messages)}"
            )

        # -------------------------------------------------------------
        # 2) events / orders / invalid rows 구성
        # -------------------------------------------------------------
        events_rows, orders_rows, invalid_rows = build_events_orders_invalid_rows(
            cur, messages, raw_ids
        )

        # -------------------------------------------------------------
        # 3) events: 가능한 한 항상 insert
        # -------------------------------------------------------------
        if events_rows:
            execute_values(
                cur,
                SQL_INSERT_EVENTS_VALUES,
                events_rows,
                page_size=min(BATCH_SIZE, 1000),
            )

        # -------------------------------------------------------------
        # 4) orders_invalid: 이상 데이터 기록
        # -------------------------------------------------------------
        if invalid_rows:
            execute_values(
                cur,
                SQL_INSERT_ORDERS_INVALID_VALUES,
                invalid_rows,
                page_size=min(BATCH_SIZE, 1000),
            )

        # -------------------------------------------------------------
        # 5) orders: 정상 데이터만 upsert (raw_reference_id 포함)
        # -------------------------------------------------------------
        if orders_rows:
            execute_values(
                cur,
                SQL_UPSERT_ORDERS_VALUES,
                orders_rows,
                page_size=min(BATCH_SIZE, 1000),
            )

        # -------------------------------------------------------------
        # 6) DB commit 성공 후 offset 커밋
        # -------------------------------------------------------------
        conn.commit()
        commit_offsets_exactly(consumer, last_offsets)

        print(
            f"✅ [BATCH OK] size={len(messages)} "
            f"(raw={len(orders_raw_rows)}, events={len(events_rows)}, orders={len(orders_rows)}, invalid={len(invalid_rows)})"
        )

    except Exception as e:
        conn.rollback()
        print(f"❌ [BATCH FAIL] size={len(messages)} error={e}")
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


# =============================================================================
# 메인 루프
# =============================================================================
def main():
    print("📨 Kafka Consumer 시작")
    print("=" * 60)
    print(f"- topic      : {KAFKA_TOPIC}")
    print(f"- bootstrap  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"- group_id   : {KAFKA_GROUP_ID}")
    print(f"- offset     : {AUTO_OFFSET_RESET}")
    print(f"- batch_size : {BATCH_SIZE}")
    print(f"- flush_sec  : {FLUSH_EVERY_SEC}")
    print("=" * 60)

    # ✅ enable_auto_commit=False : DB commit 성공 후에만 offset 커밋하려고
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        max_poll_records=BATCH_SIZE,
    )

    conn = connect_db_with_retry()

    # ✅ 배치 버퍼
    buffer = []
    last_flush_ts = time.time()

    try:
        while True:
            # -------------------------------------------------------------
            # (1) 이미 buffer가 남아있는 경우(직전 배치 실패) -> 재시도 우선
            # -------------------------------------------------------------
            if buffer:
                time_due = (time.time() - last_flush_ts) >= FLUSH_EVERY_SEC
                size_due = len(buffer) >= BATCH_SIZE

                if time_due or size_due:
                    try:
                        process_batch(conn, consumer, buffer)
                        buffer.clear()
                        last_flush_ts = time.time()
                    except Exception:
                        # 같은 buffer 유지한 채로 잠깐 쉬고 재시도
                        time.sleep(1.0)
                    continue

            # -------------------------------------------------------------
            # (2) Kafka poll로 메시지 수집
            # -------------------------------------------------------------
            records = consumer.poll(timeout_ms=200)  # 0.2초 기다렸다가 묶어서 가져오기
            for _tp, msgs in records.items():
                buffer.extend(msgs)

            if not buffer:
                continue

            # -------------------------------------------------------------
            # (3) flush 조건
            # - 1000개 이상 or 일정 시간 경과
            # -------------------------------------------------------------
            time_due = (time.time() - last_flush_ts) >= FLUSH_EVERY_SEC
            size_due = len(buffer) >= BATCH_SIZE

            if not (time_due or size_due):
                continue

            # -------------------------------------------------------------
            # (4) 배치 처리
            # -------------------------------------------------------------
            try:
                process_batch(conn, consumer, buffer)
                buffer.clear()
                last_flush_ts = time.time()
            except Exception:
                # 실패하면 buffer 유지 → 다음 루프에서 재시도
                time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n🛑 Consumer 종료")
    finally:
        # 종료 직전에 남은 buffer flush 시도 (성공 시 offset 커밋)
        if buffer:
            try:
                print(f"ℹ️ 종료 전 남은 메시지 flush 시도: {len(buffer)}건")
                process_batch(conn, consumer, buffer)
            except Exception:
                print("⚠️ 종료 전 flush 실패 (offset 미커밋 상태로 종료)")

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