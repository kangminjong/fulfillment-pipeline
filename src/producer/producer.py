import time
import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 방금 수정한 공장에서 함수 가져오기
try:
    from src.producer.data_factory import create_random_event
except ImportError:
    # 경로 문제 방지용
    from data_factory import create_random_event

# ---------------------------------------------------------
# ⚙️ 카프카 접속 설정
# ---------------------------------------------------------
if os.getenv('BOOTSTRAP_SERVERS'):
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
else:
    BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_NAME = 'event'  # 팀원과 약속한 토픽 이름

def create_producer():
    """카프카 연결 시도 (무한 재시도 로직)"""
    producer = None
    print(f"📡 카프카 브로커 연결 시도 중... ({BOOTSTRAP_SERVERS})")
    
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                # JSON 직렬화 & 한글 깨짐 방지
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            )
            print("✅ 카프카 연결 성공!")
        except NoBrokersAvailable:
            print("⏳ 브로커를 찾을 수 없습니다. 3초 후 재시도...")
            time.sleep(3)
    return producer

if __name__ == "__main__":
    producer = create_producer()
    print(f"🚀 [프로듀서] '{TOPIC_NAME}' 토픽으로 주문 데이터 전송 시작...\n")

    try:
        while True:
            # 1. 데이터 생성 (Flat JSON 형태)
            data = create_random_event()
            
            # 2. 전송
            producer.send(TOPIC_NAME, value=data)
            producer.flush() # 즉시 전송 확인
            
            # 3. 로그 출력 (데이터 구조에 맞게 수정됨)
            # 이제 data['payload'] 같은 건 없습니다. 바로 꺼내면 됩니다.
            
            status = data['current_status']
            order_id = data['order_id']
            
            # 상태별로 이모지와 출력 내용을 다르게 해서 보기 편하게 함
            if status == 'SHIPPED':
                print(f"🚚 [전송] {status} - {order_id} (운송장: {data['tracking_no']})")
                
            elif status == 'HOLD':
                print(f"⚠️ [전송] {status} - {order_id} (사유: {data['hold_reason_code']})")
                
            else: # PAYMENT_CONFIRMED 등
                print(f"✅ [전송] {status} - {order_id}")
            
            # 4. 속도 조절 (1초에 1건)
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n🛑 전송을 중단합니다.")
        producer.close()