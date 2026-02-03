import time
import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from src.producer.data_factory import create_raw_data

# ---------------------------------------------------------
# ⚙️ 카프카 접속 설정
# ---------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'event'  # 약속한 토픽 이름

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

def check_is_trap(data):
    """
    (단순 로그용) 전송되는 데이터가 함정인지 확인
    * 실제 로직 처리는 Consumer가 하지만, 여기선 로그 출력을 위해 잠시 확인만 함
    """
    if data['product_id'] == 'TEST-002':
        return True, "재고부족(TEST-002)"
    if data['customer_id'] == 'USER-9999':
        return True, "사기의심(USER-9999)"
    if len(str(data['address'])) < 5 or "?" in str(data['address']):
        return True, "주소오류"
    return False, ""

# ---------------------------------------------------------
# 🚀 메인 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    producer = create_producer()
    print(f"🚀 [프로듀서] '{TOPIC_NAME}' 토픽으로 Raw 데이터 전송 시작...\n")

    try:
        while True:
            # 1. 데이터 생성 (함정 포함된 Raw Data)
            data = create_raw_data()
            
            # 2. Kafka 전송 (묻지도 따지지도 않고 그냥 보냄)
            producer.send(TOPIC_NAME, value=data)
            producer.flush() 
            
            # 3. 로그 출력 (개발자가 알아보기 쉽게 꾸밈)
            status = data['current_status']
            product = data['product_name']
            
            # 함정 여부 확인 (로그용)
            is_trap, trap_reason = check_is_trap(data)

            if is_trap:
                # 💣 함정 데이터는 눈에 띄게 출력
                print(f"💣 [전송] {status} | {product}")
                print(f"   └─ ⚠️ 함정 발동: {trap_reason} (Consumer가 잡아야 함!)")
            else:
                # ✅ 정상 데이터
                print(f"✅ [전송] {status} | {product}")
            
            # 4. 속도 조절
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n🛑 전송을 중단합니다.")
        if producer:
            producer.close()