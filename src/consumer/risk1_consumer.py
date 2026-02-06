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

load_dotenv()

# =========================================================
# ⚙️ [SYSTEM CONFIGURATION]
# =========================================================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"), 
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"), 
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")

# 🔔 [Slack 설정]
ENABLE_SLACK = os.getenv("ENABLE_SLACK", "TRUE").lower() == "true"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# 🚨 [임계치 설정]
USER_BURST_WINDOW = 10.0; USER_BURST_LIMIT = 5
PROD_BURST_WINDOW = 1.0;  PROD_BURST_LIMIT = 5

# 🚨 [리스크 코드 정의]
CODE_VALID       = "FUL-VALID"       # 정보 누락
CODE_INV         = "FUL-INV"         # 재고 부족
CODE_FRAUD_USER  = "FUL-FRAUD-USER"  # 유저 도배
CODE_FRAUD_PROD  = "FUL-FRAUD-PROD"  # 상품 폭주
CODE_EMPTY_JSON  = "EMPTY_JSON"      # 빈 데이터

# =========================================================
# 3. SQL QUERIES
# =========================================================
SQL_INSERT_RAW = "INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) VALUES (%s, %s, %s) RETURNING raw_id"
SQL_CHECK_STOCK = "SELECT stock FROM products WHERE product_id = %s"

# [핵심 1] 에러 로그 적재 (Empty JSON 용)
SQL_INSERT_ERROR_LOG = """
    INSERT INTO pipeline_error_logs (error_type, kafka_topic, kafka_offset, occurred_at)
    VALUES (%s, %s, %s, NOW())
"""

# [핵심 2] 슬랙 로그 적재
SQL_INSERT_SLACK_LOG = """
    INSERT INTO slack_alert_log (event_id, send_status, alert_data)
    VALUES (%s, %s, %s)
"""

SQL_UPSERT_ORDER = """
    INSERT INTO orders (
        order_id, user_id, product_id, product_name, shipping_address,
        current_stage, current_status, hold_reason_code, 
        last_event_type, last_occurred_at, raw_reference_id, created_at,
        event_produced_at, latency_p_to_k_sec, latency_p_to_d_sec
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        current_status = CASE 
            WHEN orders.current_status = 'HOLD' OR EXCLUDED.current_status = 'HOLD' THEN 'HOLD' 
            ELSE EXCLUDED.current_status 
        END,
        hold_reason_code = COALESCE(orders.hold_reason_code, EXCLUDED.hold_reason_code),
        last_occurred_at = EXCLUDED.last_occurred_at;
"""

# [수정] 제공해주신 CREATE TABLE 스키마에 맞춘 7개 컬럼 매핑
SQL_INSERT_EVENT = """
    INSERT INTO events (
        event_id, order_id, event_type, current_status, reason_code, 
        occurred_at, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# [소급 적용]
SQL_QUARANTINE_USER = "UPDATE orders SET current_status='HOLD', hold_reason_code=%s WHERE user_id=%s AND created_at >= (%s - INTERVAL '1 minute')"
SQL_QUARANTINE_USER_EVT = "UPDATE events SET current_status='HOLD', reason_code=%s WHERE order_id IN (SELECT order_id FROM orders WHERE user_id=%s AND created_at >= (%s - INTERVAL '1 minute'))"
SQL_QUARANTINE_PROD = "UPDATE orders SET current_status='HOLD', hold_reason_code=%s WHERE product_id=%s AND created_at >= (%s - INTERVAL '1 minute')"
SQL_QUARANTINE_PROD_EVT = "UPDATE events SET current_status='HOLD', reason_code=%s WHERE order_id IN (SELECT order_id FROM orders WHERE product_id=%s AND created_at >= (%s - INTERVAL '1 minute'))"

# =========================================================
# 5. 핵심 로직 & 함수
# =========================================================
def parse_iso_datetime(value):
    if not value: return datetime.now(timezone.utc)
    if isinstance(value, datetime): return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

def send_slack_alert(cur, risk_reason, order_data):
    if not ENABLE_SLACK or not SLACK_WEBHOOK_URL:
        return

    order_id = order_data.get('order_id', 'UNKNOWN')
    alert_uuid = str(uuid.uuid4())

    payload = {
        "text": f"*Risk Detected! Order Blocked*",
        "attachments": [
            {
                "color": "#ff0000",
                "fields": [
                    {"title": "Reason Code", "value": risk_reason, "short": True},
                    {"title": "Order ID", "value": order_id, "short": True},
                    {"title": "URL", "value": f"http://localhost:8000/orders/{order_id}", "short": True},
                    {"title": "Time", "value": str(datetime.now()), "short": False}
                ]
            }
        ]
    }

    status = "FAIL"
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=2)
        if response.status_code == 200:
            status = "SENT"
        else:
            print(f"Slack Send Failed: {response.status_code}")
    except Exception as e:
        print(f"Slack Error: {e}")

    try:
        cur.execute(SQL_INSERT_SLACK_LOG, (
            alert_uuid, order_id, status, json.dumps(payload)
        ))
    except Exception as e:
        print(f"DB Log Error: {e}")


def check_risk_and_stock(cur, order_data, abuse_tracker, product_tracker):
    uid = str(order_data.get("user_id", "")).strip()
    pid = str(order_data.get("product_id", "")).strip()
    addr = str(order_data.get("shipping_address", "")).strip()
    curr_time = parse_iso_datetime(order_data.get("created_at"))

    if not uid or not pid or len(addr) < 5:
        return CODE_VALID

    cur.execute(SQL_CHECK_STOCK, (pid,))
    result = cur.fetchone()
    if not result: return CODE_VALID
    if result[0] <= 0: return CODE_INV

    u_q = abuse_tracker[uid]
    u_q.append(curr_time)
    sorted_times = sorted(u_q)
    u_q.clear(); u_q.extend(sorted_times)
    while u_q and (curr_time - u_q[0]).total_seconds() > USER_BURST_WINDOW: u_q.popleft()
    if len(u_q) > USER_BURST_LIMIT: return CODE_FRAUD_USER

    p_q = product_tracker[pid]
    p_q.append(curr_time)
    sorted_prod = sorted(p_q)
    p_q.clear(); p_q.extend(sorted_prod)
    while p_q and (curr_time - p_q[0]).total_seconds() > PROD_BURST_WINDOW: p_q.popleft()
    if len(p_q) > PROD_BURST_LIMIT: return CODE_FRAUD_PROD

    return None

def apply_quarantine(cur, risk_reason, order):
    at = parse_iso_datetime(order.get("created_at"))
    uid, pid = order.get("user_id"), order.get("product_id")

    if risk_reason == CODE_FRAUD_USER:
        cur.execute(SQL_QUARANTINE_USER, (CODE_FRAUD_USER, uid, at))
        cur.execute(SQL_QUARANTINE_USER_EVT, (CODE_FRAUD_USER, uid, at))
    elif risk_reason == CODE_FRAUD_PROD:
        cur.execute(SQL_QUARANTINE_PROD, (CODE_FRAUD_PROD, pid, at))
        cur.execute(SQL_QUARANTINE_PROD_EVT, (CODE_FRAUD_PROD, pid, at))

# =========================================================
# 메인 실행부
# =========================================================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    abuse_tracker = defaultdict(deque)
    product_tracker = defaultdict(deque)

    consumer = KafkaConsumer(
        TOPIC_NAME, 
        bootstrap_servers=[BOOTSTRAP_SERVERS], 
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print(f"[Consumer] Started. Monitoring: {TOPIC_NAME}")

    try:
        for message in consumer:
            order = message.value
            
            if not order:
                try:
                    with conn.cursor() as cur:
                        cur.execute(SQL_INSERT_ERROR_LOG, (
                            CODE_EMPTY_JSON, message.topic, message.offset
                        ))
                    conn.commit()
                    print(f"[WARN] Empty JSON detected & logged. Offset: {message.offset}")
                except Exception as e:
                    conn.rollback()
                    print(f"Error Logging Failed: {e}")
                
                consumer.commit()
                continue 

            try:
                with conn.cursor() as cur:
                    risk_reason = check_risk_and_stock(cur, order, abuse_tracker, product_tracker)
                    current_status = "HOLD" if risk_reason else order.get('current_status', 'PAID')
                    current_stage = order.get('current_stage', 'PAYMENT')
                    
                    t_prod = parse_iso_datetime(order.get('event_produced_at'))
                    t_cre = parse_iso_datetime(order.get('created_at'))

                    cur.execute(SQL_INSERT_RAW, (json.dumps(order), message.offset, datetime.now()))
                    raw_id = cur.fetchone()[0]

                    # 3. Orders 테이블 저장
                    cur.execute(SQL_UPSERT_ORDER, (
                        order.get('order_id'), order.get('user_id'), order.get('product_id'), 
                        order.get('product_name'), order.get('shipping_address'), 
                        current_stage, current_status, risk_reason, 
                        "ORDER_CREATED", t_cre, raw_id, t_cre, t_prod, 0.1, 0.5
                    ))
                    
                    # 4. Events 테이블 저장 (제공해주신 스키마 7개 컬럼에 맞춰 수정)
                    det_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order.get('order_id')}_{message.offset}"))
                    cur.execute(SQL_INSERT_EVENT, (
                        det_id,                 # event_id
                        order.get('order_id'),  # order_id
                        "ORDER_CREATED",        # event_type
                        current_status,         # current_status
                        risk_reason,            # reason_code
                        t_cre,                  # occurred_at
                        t_cre                   # ingested_at
                    ))

                    if risk_reason in [CODE_FRAUD_USER, CODE_FRAUD_PROD]:
                        apply_quarantine(cur, risk_reason, order)

                    if risk_reason and ENABLE_SLACK:
                        send_slack_alert(cur, risk_reason, order)

                conn.commit()
                consumer.commit()
                
                if risk_reason:
                    print(f"[BLOCK] {risk_reason} -> Order: {order.get('order_id')}")

            except Exception as e:
                conn.rollback()
                print(f"Processing Error: {e}")

    except KeyboardInterrupt:
        print("Consumer Stopped")
    finally:
        conn.close()
        consumer.close()