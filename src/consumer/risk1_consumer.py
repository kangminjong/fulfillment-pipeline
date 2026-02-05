import os
import json
import uuid
import psycopg2
import requests  # ✅ Slack 전송용
from psycopg2.extras import Json  # ✅ JSONB insert용
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from kafka import KafkaConsumer

# =========================================================
# 1. ⚙️ 설정 및 상수 정의
# =========================================================

# ✅ 팀 규칙: localhost 사용 X (env 우선, 기본은 팀 DB)
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "192.168.239.40"),
    "database": os.getenv("POSTGRES_DB", "fulfillment"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "event")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "risk-management-group")

# ✅ Slack Webhook URL (필수)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# ✅ 주문 상세 URL 프리픽스 (요구사항 그대로)
ORDER_DETAIL_URL_PREFIX = "http://localhost:8000/orders/"

KST = timezone(timedelta(hours=9))

# [리스크 감지 임계값]
USER_BURST_WINDOW = 10.0
USER_BURST_LIMIT = 5
PROD_BURST_WINDOW = 1.0
PROD_BURST_LIMIT = 4
STOCK_LIMIT = 0

# [사유 코드]
CODE_VALID = "FUL-VALID"
CODE_FRAUD_USER = "FUL-FRAUD-USER"
CODE_FRAUD_PROD = "FUL-FRAUD-PROD"
CODE_STOCK_OUT = "FUL-INV"
CODE_SYSTEM_HOLD = "SYSTEM_HOLD"

# =========================================================
# 2. 📝 SQL 쿼리 정의
# =========================================================
SQL_CHECK_STOCK = "SELECT stock FROM products WHERE product_id = %s"

SQL_INSERT_RAW = """
    INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) 
    VALUES (%s, %s, %s) RETURNING raw_id
"""

SQL_UPSERT_ORDER = """
    INSERT INTO orders (
        order_id, user_id, product_id, product_name, shipping_address,
        current_stage, current_status, hold_reason_code, 
        last_event_type, last_occurred_at, raw_reference_id, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        current_stage = EXCLUDED.current_stage,
        current_status = EXCLUDED.current_status,
        hold_reason_code = EXCLUDED.hold_reason_code,
        raw_reference_id = EXCLUDED.raw_reference_id;
"""

SQL_INSERT_EVENT = """
    INSERT INTO events (event_id, order_id, event_type, current_status, reason_code, occurred_at)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# -------------------------------------------------------------------------
# [소급 적용 SQL] Orders와 Events를 동시에 업데이트
# -------------------------------------------------------------------------
SQL_QUARANTINE_USER_ORDERS = """
    UPDATE orders 
    SET current_status = 'HOLD', 
        hold_reason_code = %s
    WHERE user_id = %s AND product_id = %s AND order_id != %s 
      AND created_at >= (%s - INTERVAL '30 seconds')
"""

SQL_QUARANTINE_USER_EVENTS = """
    UPDATE events
    SET current_status = 'HOLD',
        reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders 
        WHERE user_id = %s AND product_id = %s 
          AND created_at >= (%s - INTERVAL '30 seconds')
          AND order_id != %s
    )
"""

SQL_QUARANTINE_PROD_ORDERS = """
    UPDATE orders 
    SET current_status = 'HOLD', 
        hold_reason_code = %s
    WHERE product_id = %s AND order_id != %s
      AND created_at >= (%s - INTERVAL '5 seconds')
"""

SQL_QUARANTINE_PROD_EVENTS = """
    UPDATE events
    SET current_status = 'HOLD',
        reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders
        WHERE product_id = %s
          AND created_at >= (%s - INTERVAL '5 seconds')
          AND order_id != %s
    )
"""

# -------------------------------------------------------------------------
# ✅ Slack 알림 중복 방지 테이블(slack_alert_log) 관련 SQL
# -------------------------------------------------------------------------
SQL_SLACK_LOG_EXISTS = """
    SELECT 1 FROM slack_alert_log WHERE event_id = %s
"""

SQL_SLACK_LOG_INSERT = """
    INSERT INTO slack_alert_log (event_id, send_status, alert_data)
    VALUES (%s, %s, %s)
"""

SQL_SLACK_LOG_UPDATE_STATUS = """
    UPDATE slack_alert_log
    SET send_status = %s
    WHERE event_id = %s
"""

# =========================================================
# 3. 🛠️ 유틸리티 함수
# =========================================================
def get_kst_now():
    return datetime.now(KST)

def parse_iso_datetime(value):
    if not value:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            v = value.strip()
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except:
            pass
    return datetime.now(timezone.utc)

# =========================================================
# 4. ⚖️ 리스크 감지 로직
# =========================================================
def check_stock_anomaly(conn, pid):
    if not pid:
        return False
    with conn.cursor() as cur:
        cur.execute(SQL_CHECK_STOCK, (str(pid),))
        row = cur.fetchone()
    if row is None:
        return True
    return row[0] is not None and row[0] <= STOCK_LIMIT

def check_burst_anomaly(order_data, product_tracker):
    pid = order_data.get("product_id")
    if not pid:
        return False

    now_dt = parse_iso_datetime(order_data.get("last_occurred_at") or order_data.get("occurred_at"))
    q = product_tracker[pid]
    q.append(now_dt)

    cutoff = now_dt.timestamp() - PROD_BURST_WINDOW
    while q and q[0].timestamp() < cutoff:
        q.popleft()

    # ✅ 메모리 보호(안전장치)
    if len(q) > 5000:
        while len(q) > 5000:
            q.popleft()

    return len(q) >= PROD_BURST_LIMIT

# =========================================================
# 5. 🛡️ 메인 리스크 판단
# =========================================================
def check_risk(order_data, abuse_tracker, product_tracker, conn):
    uid = str(order_data.get("user_id", ""))
    pid = str(order_data.get("product_id", ""))
    addr = str(order_data.get("shipping_address", ""))
    curr_time = get_kst_now()

    # 1) 필수값(정보 누락) 판단
    bad_keywords = ["?", "Unknown", "123", "NULL"]
    if (not uid) or (not addr) or len(addr) < 5 or any(k in addr for k in bad_keywords):
        # ⚠️ 너희 코드에선 이 경우 CODE_VALID를 리턴하고,
        # 아래 is_hold 계산에서 risk_reason이 truthy면 HOLD로 처리됨.
        # (즉 "정보 누락도 HOLD로 만들겠다"는 정책이면 이대로 OK)
        return CODE_VALID

    # 2) 유저 도배
    user_risk_detected = False
    key = (uid, pid)

    if key not in abuse_tracker:
        abuse_tracker[key] = {"count": 1, "start_time": curr_time}
    else:
        record = abuse_tracker[key]
        elapsed = (curr_time - record["start_time"]).total_seconds()

        if elapsed > USER_BURST_WINDOW:
            abuse_tracker[key] = {"count": 1, "start_time": curr_time}
        else:
            record["count"] += 1
            if record["count"] > USER_BURST_LIMIT:
                user_risk_detected = True

    # 3) 상품 폭주
    prod_risk_detected = check_burst_anomaly(order_data, product_tracker)

    # 결정
    if user_risk_detected:
        return CODE_FRAUD_USER
    if check_stock_anomaly(conn, pid):
        return CODE_STOCK_OUT
    if prod_risk_detected:
        return CODE_FRAUD_PROD

    return None

# =========================================================
# 6. 💾 DB 처리 (Events 포함 업데이트)
#    + ✅ event_id를 밖에서 쓰도록 반환하도록 변경
# =========================================================
def save_to_db(cur, data, is_hold, risk_reason, kafka_offset):
    current_timestamp = get_kst_now()
    target_stage = data.get("current_stage", "PAYMENT")

    if is_hold:
        target_status = "HOLD"
        final_reason = risk_reason if risk_reason else CODE_SYSTEM_HOLD
    else:
        target_status = data.get("current_status", "PAID")
        final_reason = None

    # 1) orders_raw 적재
    cur.execute(SQL_INSERT_RAW, (json.dumps(data, ensure_ascii=False), kafka_offset, current_timestamp))
    raw_id = cur.fetchone()[0]

    # 2) orders upsert
    cur.execute(
        SQL_UPSERT_ORDER,
        (
            data["order_id"],
            data["user_id"],
            data["product_id"],
            data["product_name"],
            data["shipping_address"],
            target_stage,
            target_status,
            final_reason,
            data.get("last_event_type", "ORDER_CREATED"),
            data.get("last_occurred_at"),
            raw_id,
            current_timestamp,
        ),
    )

    # 3) events insert (✅ event_id 생성 후 반환)
    event_id = str(uuid.uuid4())
    cur.execute(
        SQL_INSERT_EVENT,
        (
            event_id,
            data["order_id"],
            target_stage,      # event_type로 stage를 넣는 현재 구조 유지
            target_status,     # current_status
            final_reason,      # reason_code
            current_timestamp,
        ),
    )

    return target_status, final_reason, event_id

# =========================================================
# 7. 🧨 소급 HOLD 처리(기존 유지)
# =========================================================
def quarantine_retroactive_user(cur, uid, pid, current_order_id):
    current_timestamp = get_kst_now()

    cur.execute(
        SQL_QUARANTINE_USER_ORDERS,
        (CODE_FRAUD_USER, str(uid), str(pid), str(current_order_id), current_timestamp),
    )
    row_count = cur.rowcount

    cur.execute(
        SQL_QUARANTINE_USER_EVENTS,
        (CODE_FRAUD_USER, str(uid), str(pid), current_timestamp, str(current_order_id)),
    )

    return row_count

def quarantine_retroactive_prod(cur, pid, current_order_id):
    current_timestamp = get_kst_now()

    cur.execute(
        SQL_QUARANTINE_PROD_ORDERS,
        (CODE_FRAUD_PROD, str(pid), str(current_order_id), current_timestamp),
    )
    row_count = cur.rowcount

    cur.execute(
        SQL_QUARANTINE_PROD_EVENTS,
        (CODE_FRAUD_PROD, str(pid), current_timestamp, str(current_order_id)),
    )

    return row_count

# =========================================================
# 8. 🔔 Slack 알림(중복 방지 + 로그)
# =========================================================
def slack_log_exists(cur, event_id: str) -> bool:
    cur.execute(SQL_SLACK_LOG_EXISTS, (event_id,))
    return cur.fetchone() is not None

def slack_log_insert_pending(cur, event_id: str, alert_data: dict):
    # ✅ 먼저 로그를 남겨서 "중복 방지"를 확실히 걸어둠
    cur.execute(SQL_SLACK_LOG_INSERT, (event_id, "PENDING", Json(alert_data)))

def slack_log_update(cur, event_id: str, status: str):
    cur.execute(SQL_SLACK_LOG_UPDATE_STATUS, (status, event_id))

def build_slack_payload(order: dict, event_id: str, reason_code: str):
    """
    ✅ Slack에 보낼 데이터(= alert_data JSONB로 저장될 내용)
    - 일단 최대한 많이 넣어두면 시연/디버깅이 쉬움
    """
    order_id = order.get("order_id")
    order_url = f"{ORDER_DETAIL_URL_PREFIX}{order_id}" if order_id else None

    return {
        "event_id": event_id,
        "order_id": order_id,
        "url": order_url,  # ✅ user_id 대신 url
        "product_id": order.get("product_id"),
        "product_name": order.get("product_name"),
        "shipping_address": order.get("shipping_address"),
        "reason_code": reason_code,
        "last_event_type": order.get("last_event_type"),
        "last_occurred_at": order.get("last_occurred_at") or order.get("occurred_at"),
        "detected_at_kst": get_kst_now().isoformat(),
    }

def send_slack_webhook(alert_data: dict) -> bool:
    """
    ✅ Slack Webhook 전송
    - 성공 True / 실패 False
    """
    if not SLACK_WEBHOOK_URL:
        # 환경변수 없으면 실패로 처리(실서비스면 raise해도 됨)
        return False

    # Slack Incoming Webhook 기본 포맷: {"text": "..."}
    text = (
        f"🛑 *HOLD 발생*\n"
        f"- event_id: `{alert_data.get('event_id')}`\n"
        f"- order_id: `{alert_data.get('order_id')}`\n"
        f"- product: {alert_data.get('product_name')} ({alert_data.get('product_id')})\n"
        f"- url: {alert_data.get('url')}\n"
        f"- reason: *{alert_data.get('reason_code')}*\n"
        f"- at(KST): {alert_data.get('detected_at_kst')}"
    )

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"text": text}, ensure_ascii=False).encode("utf-8"),
            timeout=5,
        )
        return 200 <= resp.status_code < 300
    except Exception:
        return False

# =========================================================
# 9. 🚀 실행
# =========================================================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    abuse_tracker = {}
    product_rate_tracker = defaultdict(lambda: deque())

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("📡 [Risk Consumer] HOLD 발생 시 Slack 알림 + 중복방지(slack_alert_log) 적용")

    try:
        for message in consumer:
            order = message.value

            # ✅ 리스크 판단
            risk_reason = check_risk(order, abuse_tracker, product_rate_tracker, conn)

            # ✅ 정책: risk_reason가 있거나, 들어온 이벤트 자체가 HOLD면 HOLD로 처리
            is_hold = True if (risk_reason or order.get("current_stage") == "HOLD") else False

            try:
                with conn.cursor() as cur:
                    # 1) DB 저장
                    final_status, final_reason, event_id = save_to_db(
                        cur, order, is_hold, risk_reason, message.offset
                    )

                    # 2) 소급 적용(기존 유지)
                    if risk_reason == CODE_FRAUD_USER:
                        count = quarantine_retroactive_user(
                            cur, order["user_id"], order["product_id"], order["order_id"]
                        )
                        if count > 0:
                            print(f"🚩 [QUARANTINE-USER] {count}건 (Orders+Events) 강제 HOLD")

                    elif risk_reason == CODE_FRAUD_PROD:
                        count = quarantine_retroactive_prod(
                            cur, order["product_id"], order["order_id"]
                        )
                        if count > 0:
                            print(f"🚩 [QUARANTINE-PROD] {count}건 (Orders+Events) 강제 HOLD")

                    # 3) ✅ HOLD면 Slack 알림 (중복방지)
                    if final_status == "HOLD":
                        alert_data = build_slack_payload(order, event_id, final_reason)

                        # 이미 이 이벤트(event_id)에 대해 알림 기록 있으면 스킵
                        if not slack_log_exists(cur, event_id):
                            # (A) 먼저 PENDING으로 기록(중복 방지)
                            slack_log_insert_pending(cur, event_id, alert_data)

                            # (B) 슬랙 전송
                            ok = send_slack_webhook(alert_data)

                            # (C) 상태 업데이트
                            slack_log_update(cur, event_id, "SENT" if ok else "FAIL")

                            print(f"🔔 [SLACK] event_id={event_id} status={'SENT' if ok else 'FAIL'}")
                        else:
                            print(f"🔁 [SLACK-SKIP] 이미 기록된 event_id={event_id}")

                    # 4) 커밋
                    conn.commit()

                print(f"[{final_status}] {order.get('order_id')} | Reason: {risk_reason}")

            except Exception as e:
                conn.rollback()
                print(f"❌ DB Error: {e}")

    except KeyboardInterrupt:
        conn.close()
        consumer.close()