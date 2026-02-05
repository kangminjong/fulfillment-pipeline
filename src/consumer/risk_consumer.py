import json
import uuid
import psycopg2
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer

# ---------------------------------------------------------
# ⚙️ DB 및 Kafka 설정
# ---------------------------------------------------------
DB_CONFIG = {
    "host": "192.168.239.40",
    "database": "fulfillment",
    "user": "admin",
    "password": "admin"
}
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'event'
GROUP_ID = 'risk-management-group'

KST = timezone(timedelta(hours=9))

def get_kst_now():
    return datetime.now(KST)

# ---------------------------------------------------------
# ⚖️ 리스크 판단 로직 (수정: 사유 코드 변경)
# ---------------------------------------------------------
def check_risk(order_data, tracker):
    uid = str(order_data.get('user_id', ''))
    pid = str(order_data.get('product_id', ''))
    addr = str(order_data.get('shipping_address', ''))
    curr_time = get_kst_now()

    # 1. 주소지 유효성 검사
    bad_keywords = ["?", "Unknown", "123", "NULL"]
    if not uid or not addr or len(addr) < 5 or any(k in addr for k in bad_keywords):
        return 'FUL-VALID'

    # 2. [수정] 동일 유저/상품 도배 검사 (FUL-FRAUD-USER)
    key = (uid, pid)
    if key not in tracker:
        tracker[key] = {'count': 1, 'start_time': curr_time}
    else:
        record = tracker[key]
        elapsed = (curr_time - record['start_time']).total_seconds()
        
        if elapsed > 10.0:  # 10초 윈도우
            tracker[key] = {'count': 1, 'start_time': curr_time}
        else:
            record['count'] += 1
            if record['count'] > 5:  # 5회 초과 시
                return 'FUL-FRAUD-USER'  # <-- 사유 코드 수정
    
    return None

# ---------------------------------------------------------
# 💾 DB 저장 로직 (Stage 유지, Status=HOLD)
# ---------------------------------------------------------
def save_to_db(cur, data, is_hold, risk_reason, kafka_offset):
    current_timestamp = get_kst_now()
    
    # Stage는 원본 유지, Status는 조건에 따라 HOLD
    target_stage = data.get('current_stage', 'PAYMENT')
    if is_hold:
        target_status = 'HOLD'
        final_reason = risk_reason if risk_reason else 'SYSTEM_HOLD'
    else:
        target_status = data.get('current_status', 'PAID')
        final_reason = None

    # [Step 1] Raw 데이터 저장
    cur.execute("""
        INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) 
        VALUES (%s, %s, %s) RETURNING raw_id
    """, (json.dumps(data, ensure_ascii=False), kafka_offset, current_timestamp))
    raw_id = cur.fetchone()[0]

    # [Step 2] Orders 테이블 (Stage 보존형)
    cur.execute("""
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
    """, (
        data['order_id'], data['user_id'], data['product_id'], data['product_name'], data['shipping_address'],
        target_stage, target_status, final_reason,
        data.get('last_event_type', 'ORDER_CREATED'), data.get('last_occurred_at'),
        raw_id, current_timestamp
    ))

    # [Step 3] Events 로그
    cur.execute("""
        INSERT INTO events (event_id, order_id, event_type, current_status, reason_code, occurred_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (str(uuid.uuid4()), data['order_id'], data.get('last_event_type', target_stage), 
          target_status, final_reason, current_timestamp))
    
    return target_status

# ---------------------------------------------------------
# 🚨 소급 격리 로직 (FUL-FRAUD-USER 대응)
# ---------------------------------------------------------
def quarantine_retroactive(cur, uid, pid, current_order_id):
    current_timestamp = get_kst_now()
    reason_code = 'FUL-FRAUD-USER'  # <-- 수정
    
    cur.execute("""
        UPDATE orders 
        SET current_status = 'HOLD', 
            hold_reason_code = %s
        WHERE user_id = %s AND product_id = %s AND order_id != %s 
          AND created_at >= (%s - INTERVAL '15 seconds')
          AND current_status != 'HOLD'
    """, (reason_code, str(uid), str(pid), str(current_order_id), current_timestamp))
    
    return cur.rowcount

# ---------------------------------------------------------
# 🚀 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False 
    
    # abuse_tracker 초기화 (보통 전역 혹은 메인에서 관리)
    abuse_tracker = {}

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"📡 [Risk Consumer] 감시 중... (도배 사유: FUL-FRAUD-USER)")

    try:
        for message in consumer:
            order = message.value
            risk_reason = check_risk(order, abuse_tracker)
            is_hold = True if (risk_reason or order.get('current_stage') == 'HOLD') else False

            try:
                with conn.cursor() as cur:
                    final_status = save_to_db(cur, order, is_hold, risk_reason, message.offset)

                    # 소급 적용: 새로 바뀐 사유 코드(FUL-FRAUD-USER)로 체크
                    if risk_reason == 'FUL-FRAUD-USER':
                        count = quarantine_retroactive(cur, order['user_id'], order['product_id'], order['order_id'])
                        if count > 0:
                            print(f"🚩 [QUARANTINE] 유저 {order['user_id']}의 과거 주문 {count}건 소급 HOLD 완료")

                    conn.commit()
                print(f"[{final_status}] {order['order_id']} | Stage: {order.get('current_stage')} | Reason: {risk_reason}")

            except Exception as e:
                conn.rollback()
                print(f"❌ DB Error: {e}")

    except KeyboardInterrupt:
        conn.close()
        consumer.close()