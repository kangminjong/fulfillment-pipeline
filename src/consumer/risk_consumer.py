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

# Rate Limiting 설정 (10초에 5회 초과 시 차단)
TIME_WINDOW_SECONDS = 10.0
MAX_ORDER_COUNT = 5
abuse_tracker = {}

def get_kst_now():
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST)

# ---------------------------------------------------------
# ⚖️ 리스크 판단 로직
# ---------------------------------------------------------
def check_risk(order_data, tracker):
    uid = str(order_data.get('user_id', ''))
    pid = str(order_data.get('product_id', ''))
    
    addr_raw = order_data.get('shipping_address')
    addr = str(addr_raw) if addr_raw is not None else ""
    
    curr_time = get_kst_now()

    # 1. [FUL-VALID]
    bad_keywords = ["?", "Unknown", "123", "NULL"]
    if not uid or not addr or len(addr) < 5 or any(k in addr for k in bad_keywords):
        return 'FUL-VALID'

    # 2. [FUL-FRAUD-PROD]
    key = (uid, pid)
    
    if key not in tracker:
        tracker[key] = {'count': 1, 'start_time': curr_time}
    else:
        record = tracker[key]
        elapsed = (curr_time - record['start_time']).total_seconds()
        
        if elapsed > TIME_WINDOW_SECONDS:
            tracker[key] = {'count': 1, 'start_time': curr_time}
        else:
            record['count'] += 1
            if record['count'] > MAX_ORDER_COUNT:
                return 'FUL-FRAUD-PROD'
    
    return None

# ---------------------------------------------------------
# 💾 DB 저장 로직 (Stage <-> Status 변경)
# ---------------------------------------------------------
def save_to_db(cur, data, status, reason=None, kafka_offset=0):
    current_timestamp = get_kst_now()

    # [Step 1] Bronze Layer
    cur.execute("""
        INSERT INTO orders_raw (raw_payload, kafka_offset, ingested_at) 
        VALUES (%s, %s, %s) 
        RETURNING raw_id
    """, (json.dumps(data, ensure_ascii=False), kafka_offset, current_timestamp))
    
    raw_id = cur.fetchone()[0]

    final_reason = reason
    
    # 기본값
    db_stage = data.get('current_stage', 'PAYMENT')
    db_status = status 

    if status == 'HOLD':
        db_stage = 'HOLD'          
        db_status = 'RISK_CHECK'   
        
        if final_reason is None:
            final_reason = 'RANDOM_INSPECTION'
    else:
        final_reason = None

    # [Step 2] Silver Layer (Orders)
    cur.execute("""
        INSERT INTO orders (
            order_id, product_id, product_name, current_stage, current_status, 
            hold_reason_code, last_event_type, last_occurred_at, shipping_address, user_id, 
            raw_reference_id, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            current_stage = EXCLUDED.current_stage,
            current_status = EXCLUDED.current_status,
            hold_reason_code = EXCLUDED.hold_reason_code,
            raw_reference_id = EXCLUDED.raw_reference_id,
            created_at = %s
    """, (
        data['order_id'], data['product_id'], data['product_name'], 
        db_stage, 
        db_status, 
        final_reason, data.get('last_event_type', 'ORDER_CREATED'), current_timestamp, 
        data['shipping_address'], data['user_id'], raw_id, current_timestamp, current_timestamp
    ))

    # [Step 3] Audit Log (Events) - 현재 주문 저장
    cur.execute("""
        INSERT INTO events (
            event_id, order_id, event_type, current_status, reason_code, occurred_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        str(uuid.uuid4()), data['order_id'], 
        db_stage,   # HOLD
        db_status,  # RISK_CHECK
        final_reason, current_timestamp
    ))
    
    return final_reason

# ---------------------------------------------------------
# 🚨 [수정됨] 소급 적용 로직 (UPDATE 방식)
# ---------------------------------------------------------
def quarantine_retroactive(cur, uid, pid, current_order_id):
    """
    현재 적발된 주문(current_order_id)을 제외한 과거 주문들을 찾아서
    Events 테이블의 기록을 'HOLD'로 덮어씁니다. (INSERT 아님!)
    """
    current_timestamp = get_kst_now()
    reason_code = 'FUL-FRAUD-PROD'
    
    # 1. orders 테이블 수정 (Snapshot 업데이트)
    cur.execute("""
        UPDATE orders 
        SET current_stage = 'HOLD',
            current_status = 'RISK_CHECK',
            hold_reason_code = %s,
            created_at = %s
        WHERE user_id = %s 
        AND product_id = %s 
        AND order_id != %s 
        AND created_at >= (%s - INTERVAL '15 seconds')
    """, (reason_code, current_timestamp, str(uid), str(pid), str(current_order_id), current_timestamp))
    
    count = cur.rowcount

    # 2. events 테이블 수정 (Log 덮어쓰기)
    # 과거에 정상(NULL)이라고 기록했던 행들을 찾아서 -> HOLD로 변경
    if count > 0:
        cur.execute("""
            UPDATE events
            SET event_type = 'HOLD',        -- 기존 상태 지움
                current_status = 'RISK_CHECK',
                reason_code = %s,           -- 이유 기록 (FUL-FRAUD-PROD)
                occurred_at = %s            -- (선택사항) 시간도 현재 적발 시간으로 맞춤
            WHERE order_id IN (
                SELECT order_id 
                FROM orders
                WHERE user_id = %s 
                AND product_id = %s
                AND order_id != %s
                AND created_at >= (%s - INTERVAL '15 seconds')
            )
            AND reason_code IS NULL         -- (중요) 정상이었던 데이터만 수정
        """, (reason_code, current_timestamp, str(uid), str(pid), str(current_order_id), current_timestamp))
    
    return count

# ---------------------------------------------------------
# 🚀 메인 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False 

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"📡 [Risk Consumer] 과거 이력 덮어쓰기 모드 (All HOLD)")

    try:
        for message in consumer:
            order = message.value
            
            risk_reason = check_risk(order, abuse_tracker)
            
            current_status_in_data = order.get('current_status', 'PAID')
            final_status = 'HOLD' if (risk_reason or current_status_in_data == 'HOLD') else current_status_in_data

            try:
                with conn.cursor() as cur:
                    actual_reason = save_to_db(cur, order, final_status, risk_reason, message.offset)

                    if risk_reason == 'FUL-FRAUD-PROD':
                        # 🚨 소급 적용 (이제 events 테이블을 덮어씁니다)
                        count = quarantine_retroactive(
                            cur, 
                            order['user_id'], 
                            order['product_id'], 
                            order['order_id']
                        )
                        if count > 0:
                            print(f"🚨 [QUARANTINE] {order['user_id']} | 과거 {count}건 이력 수정 완료 (All HOLD)")

                    conn.commit()
                
                if final_status == 'HOLD':
                    print(f"🛑 [HOLD] {order.get('order_id')} | 사유: {actual_reason}")
                else:
                    print(f"✅ [{final_status}] {order.get('order_id')} | {order.get('product_name')}")

            except Exception as e:
                conn.rollback()
                print(f"🔥 DB Error (Order: {order.get('order_id')}): {e}")

    except KeyboardInterrupt:
        print("🛑 컨슈머를 종료합니다.")
        conn.close()
        consumer.close()