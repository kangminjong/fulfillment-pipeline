import os
import json
import uuid
import psycopg2
import requests
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from kafka import KafkaConsumer
from dotenv import load_dotenv

# 1. 환경 변수 로드
load_dotenv()

# =========================================================
# ⚙️ [SYSTEM CONFIGURATION]
# =========================================================
BATCH_SIZE = 1           # 1개만 와도 즉시 처리 (테스트 모드)
POLL_TIMEOUT = 500       # 0.5초 대기
ENABLE_SLACK = False     # False: 로그만 출력, True: 실제 발송

# =========================================================
# 2. CONSTANTS & CONNECTIONS
# =========================================================
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
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
ORDER_DETAIL_URL_PREFIX = "http://localhost:8000/orders/"

KST = timezone(timedelta(hours=9))

# Thresholds
USER_BURST_WINDOW = 10.0; USER_BURST_LIMIT = 5
PROD_BURST_WINDOW = 1.0; PROD_BURST_LIMIT = 4
STOCK_LIMIT = 0

# Reason Codes
CODE_VALID = "FUL-VALID"
CODE_FRAUD_USER = "FUL-FRAUD-USER"
CODE_FRAUD_PROD = "FUL-FRAUD-PROD"
CODE_STOCK_OUT = "FUL-INV"
CODE_SYSTEM_HOLD = "SYSTEM_HOLD"

# =========================================================
# 3. SQL QUERIES
# =========================================================
SQL_CHECK_STOCK = "SELECT stock FROM products WHERE product_id = %s"
SQL_CHECK_RAW_EXIST = "SELECT raw_id FROM orders_raw WHERE kafka_offset = %s"
SQL_INSERT_RAW = "INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) VALUES (%s, %s, %s) RETURNING raw_id"

# Orders (Upsert)
SQL_UPSERT_ORDER = """
    INSERT INTO orders (
        order_id, user_id, product_id, product_name, shipping_address,
        current_stage, current_status, hold_reason_code, 
        last_event_type, last_occurred_at, raw_reference_id, created_at,
        event_produced_at, latency_p_to_k_sec, latency_p_to_d_sec
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        current_stage = EXCLUDED.current_stage,
        current_status = EXCLUDED.current_status,
        hold_reason_code = EXCLUDED.hold_reason_code,
        raw_reference_id = EXCLUDED.raw_reference_id,
        last_occurred_at = EXCLUDED.last_occurred_at,
        event_produced_at = EXCLUDED.event_produced_at,
        latency_p_to_d_sec = EXCLUDED.latency_p_to_d_sec;
"""

# Events (Idempotency)
SQL_INSERT_EVENT_SAFE = """
    INSERT INTO events (event_id, order_id, event_type, current_status, reason_code, occurred_at, ingested_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
"""

# Retroactive SQLs
SQL_QUARANTINE_USER_ORDERS = """
    UPDATE orders SET current_status = 'HOLD', hold_reason_code = %s
    WHERE user_id = %s AND product_id = %s AND order_id != %s 
      AND created_at >= (%s - INTERVAL '30 seconds')
"""
SQL_QUARANTINE_USER_EVENTS = """
    UPDATE events SET current_status = 'HOLD', reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders WHERE user_id = %s AND product_id = %s 
          AND created_at >= (%s - INTERVAL '30 seconds') AND order_id != %s
    )
"""
SQL_QUARANTINE_PROD_ORDERS = """
    UPDATE orders SET current_status = 'HOLD', hold_reason_code = %s
    WHERE product_id = %s AND order_id != %s AND created_at >= (%s - INTERVAL '5 seconds')
"""
SQL_QUARANTINE_PROD_EVENTS = """
    UPDATE events SET current_status = 'HOLD', reason_code = %s
    WHERE order_id IN (
        SELECT order_id FROM orders WHERE product_id = %s 
          AND created_at >= (%s - INTERVAL '5 seconds') AND order_id != %s
    )
"""

# =========================================================
# 4. HELPER FUNCTIONS
# =========================================================
def parse_iso_datetime(value):
    if not value: return datetime.now(timezone.utc)
    if isinstance(value, datetime): return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try: return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except: pass
    return datetime.now(timezone.utc)

def handle_error_and_alert(cursor, error_type, topic, offset):
    """
    Log errors to DB and send Slack alerts (if enabled).
    """
    # 1. Log to DB
    try:
        sql = "INSERT INTO pipeline_error_logs (error_type, kafka_topic, kafka_offset) VALUES (%s, %s, %s)"
        cursor.execute(sql, (error_type, topic, offset))
    except Exception as e:
        print(f"[ERROR] Failed to log error to DB: {e}")

    # 2. Send Slack Alert
    kst_now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    
    if ENABLE_SLACK and SLACK_WEBHOOK_URL:
        try:
            payload = {
                "text": f"Data Pipeline Alert: {error_type}",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": "Data Pipeline Alert", "emoji": False}
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Error Type:*\n`{error_type}`"},
                            {"type": "mrkdwn", "text": f"*Time (KST):*\n{kst_now}"},
                            {"type": "mrkdwn", "text": f"*Topic:*\n{topic}"},
                            {"type": "mrkdwn", "text": f"*Offset:*\n{offset}"}
                        ]
                    }
                ]
            }
            requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=2)
            print(f"[ALERT] Slack message sent. (Type: {error_type})")
        except Exception as e:
            print(f"[ERROR] Slack sending failed: {e}")
    else:
        print(f"[INFO] Slack alert skipped (Enabled: {ENABLE_SLACK}). Error: {error_type}")

# =========================================================
# 5. RISK LOGIC
# =========================================================
def check_stock_anomaly(conn, pid):
    if not pid: return False
    with conn.cursor() as cur:
        cur.execute(SQL_CHECK_STOCK, (str(pid),))
        row = cur.fetchone()
    if row is None: return True
    return row[0] is not None and row[0] <= STOCK_LIMIT

def check_burst_anomaly(order_data, product_tracker):
    pid = order_data.get("product_id")
    if not pid: return False
    now_dt = parse_iso_datetime(order_data.get("created_at"))
    q = product_tracker[pid]
    q.append(now_dt)
    while q and q[0].timestamp() < (now_dt.timestamp() - PROD_BURST_WINDOW): q.popleft()
    return len(q) >= PROD_BURST_LIMIT

def check_risk(order_data, abuse_tracker, product_tracker, conn):
    uid = str(order_data.get("user_id", "")); pid = str(order_data.get("product_id", ""))
    addr = str(order_data.get("shipping_address", ""))
    curr_time = parse_iso_datetime(order_data.get("created_at"))

    if (not uid) or (not addr) or len(addr) < 5: return CODE_VALID

    user_risk_detected = False
    key = (uid, pid)
    if key not in abuse_tracker: abuse_tracker[key] = {"count": 1, "start_time": curr_time}
    else:
        record = abuse_tracker[key]
        if (curr_time - record["start_time"]).total_seconds() > USER_BURST_WINDOW:
            abuse_tracker[key] = {"count": 1, "start_time": curr_time}
        else:
            record["count"] += 1
            if record["count"] > USER_BURST_LIMIT: user_risk_detected = True

    prod_risk_detected = check_burst_anomaly(order_data, product_tracker)

    if user_risk_detected: return CODE_FRAUD_USER
    if check_stock_anomaly(conn, pid): return CODE_STOCK_OUT
    if prod_risk_detected: return CODE_FRAUD_PROD
    return None

# =========================================================
# 6. DB SAVE (IDEMPOTENCY)
# =========================================================
def save_to_db(cur, data, is_hold, risk_reason, kafka_offset):
    t0 = parse_iso_datetime(data.get('event_produced_at'))
    t2 = parse_iso_datetime(data.get('created_at'))
    
    total_diff = t2.timestamp() - t0.timestamp()
    latency_d = round(max(0.0, total_diff), 4)
    latency_k = round(latency_d * 0.3, 4)

    target_stage = data.get("current_stage", "PAYMENT")
    if is_hold: target_status = "HOLD"; final_reason = risk_reason if risk_reason else CODE_SYSTEM_HOLD
    else: target_status = data.get("current_status", "PAID"); final_reason = None

    # Check Raw Data (Idempotency)
    cur.execute(SQL_CHECK_RAW_EXIST, (kafka_offset,))
    existing_raw = cur.fetchone()
    
    if existing_raw:
        raw_id = existing_raw[0]
    else:
        cur.execute(SQL_INSERT_RAW, (json.dumps(data, ensure_ascii=False), kafka_offset, t2))
        raw_id = cur.fetchone()[0]

    # Check Orders (Upsert)
    cur.execute(SQL_UPSERT_ORDER, (
        data["order_id"], data["user_id"], data["product_id"], data["product_name"], data["shipping_address"],
        target_stage, target_status, final_reason,
        data.get("last_event_type", "ORDER_CREATED"), t2, raw_id, t2, t0, latency_k, latency_d
    ))

    # Check Events (UUID5)
    unique_seed = f"{data['order_id']}_offset_{kafka_offset}"
    deterministic_event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_seed))
    cur.execute(SQL_INSERT_EVENT_SAFE, (deterministic_event_id, data["order_id"], target_stage, target_status, final_reason, t2, t2))
    
    return target_status, final_reason, deterministic_event_id

def quarantine_retroactive_user(cur, uid, pid, current_order_id, current_created_at):
    anchor_time = parse_iso_datetime(current_created_at)
    cur.execute(SQL_QUARANTINE_USER_ORDERS, (CODE_FRAUD_USER, str(uid), str(pid), str(current_order_id), anchor_time))
    cur.execute(SQL_QUARANTINE_USER_EVENTS, (CODE_FRAUD_USER, str(uid), str(pid), anchor_time, str(current_order_id)))

def quarantine_retroactive_prod(cur, pid, current_order_id, current_created_at):
    anchor_time = parse_iso_datetime(current_created_at)
    cur.execute(SQL_QUARANTINE_PROD_ORDERS, (CODE_FRAUD_PROD, str(pid), str(current_order_id), anchor_time))
    cur.execute(SQL_QUARANTINE_PROD_EVENTS, (CODE_FRAUD_PROD, str(pid), anchor_time, str(current_order_id)))

# =========================================================
# 9. MAIN EXECUTION
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
        enable_auto_commit=False, 
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print("-" * 60)
    print(f"[SYSTEM] Risk Management Consumer Started")
    print(f"[CONFIG] Batch Size: {BATCH_SIZE} | Slack Alert: {ENABLE_SLACK}")
    print("-" * 60)

    message_buffer = []

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT)
            
            if not msg_pack:
                continue 

            for partition, messages in msg_pack.items():
                for msg in messages:
                    message_buffer.append(msg)

            current_size = len(message_buffer)

            if current_size < BATCH_SIZE:
                continue 

            print(f"\n[INFO] Processing Batch... (Count: {current_size})")
            
            processed_count = 0
            skipped_count = 0

            try:
                with conn.cursor() as cur:
                    for message in message_buffer:
                        order = message.value 
                        
                        # [GUARD] Check for Empty JSON
                        if not order: 
                            print(f"[WARN] Empty JSON detected - Skipping (Offset: {message.offset})")
                            handle_error_and_alert(cur, "EMPTY_JSON", message.topic, message.offset)
                            skipped_count += 1
                            continue 

                        # [LOGIC] Check Risk
                        risk_reason = check_risk(order, abuse_tracker, product_rate_tracker, conn)
                        is_hold = True if (risk_reason or order.get("current_stage") == "HOLD") else False

                        save_to_db(cur, order, is_hold, risk_reason, message.offset)

                        if risk_reason == CODE_FRAUD_USER:
                            quarantine_retroactive_user(cur, order["user_id"], order["product_id"], order["order_id"], order.get("created_at"))
                        elif risk_reason == CODE_FRAUD_PROD:
                            quarantine_retroactive_prod(cur, order["product_id"], order["order_id"], order.get("created_at"))
                        
                        processed_count += 1
                    
                    conn.commit()
                    consumer.commit()
                    
                    print(f"[SUCCESS] Batch Complete - Processed: {processed_count}, Skipped: {skipped_count}")
                    message_buffer = []

            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Batch Processing Failed: {e}")
                message_buffer = []

    except KeyboardInterrupt:
        conn.close()
        consumer.close()
        print("\n[SYSTEM] Consumer Stopped.")