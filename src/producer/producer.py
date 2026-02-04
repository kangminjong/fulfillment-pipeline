import time
import json
import os
from kafka import KafkaProducer
from src.producer.data_factory import OrderGenerator # 프로젝트 구조에 맞게 import 경로 확인

# ---------------------------------------------------------
# ⚙️ 설정 및 속도 제어
# ---------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'event'

# [현재 설정] 테스트를 위해 1개씩 천천히 전송하는 모드
SCENARIO_DELAY = 1.0   # 시나리오 간 대기 시간 (1초)
BURST_COUNT = 1        # 기본 생성 개수 (1개)
BURST_INTERVAL = 0.5   # 메시지 간 간격 (0.5초)

"""
🚀 [나중용] 10,000건 대량 부하 테스트 시 아래 설정으로 변경하세요:
SCENARIO_DELAY = 0
BURST_COUNT = 10000
BURST_INTERVAL = 0
"""

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            acks=1,
            linger_ms=10
        )
        print("✅ 카프카 연결 성공!")
        return producer
    except Exception as e:
        print(f"❌ 카프카 연결 실패: {e}")
        return None

if __name__ == "__main__":
    gen = OrderGenerator()
    producer = create_producer()
    
    if not producer:
        exit(1)

    # 💡 실행할 시나리오 순서 정의 (재고 오류 포함)
    SCENARIO_SEQUENCE = [
        "NORMAL", 
        # "VALID_ERROR", 
        # "OUT_OF_STOCK",  # 재고 오류 시나리오 추가
        # "USER_ABUSE", 
        # "PRODUCT_BURST"
    ]

    print(f"🚀 [프로듀서] 제어 모드 실행 중... (시나리오 순서: {SCENARIO_SEQUENCE})")

    try:
        while True:
            for mode in SCENARIO_SEQUENCE:
                batch_data = []

                # --- [시나리오 선택 로직] producer가 호출 결정 ---
                if mode == "NORMAL":
                    batch_data = gen.generate_normal()
                    print("✅ [NORMAL] 정상 주문 전송")
                
                elif mode == "VALID_ERROR":
                    batch_data = gen.generate_validation_error()
                    print("❌ [VALID_ERROR] 결함 데이터 전송")
                
                elif mode == "OUT_OF_STOCK":
                    # 💡 재고 오류 메서드 호출
                    batch_data = gen.generate_out_of_stock()
                    print("📦 [OUT_OF_STOCK] 재고 부족 시나리오 전송")
                
                elif mode == "USER_ABUSE":
                    batch_data = gen.generate_user_burst(count=BURST_COUNT)
                    print(f"🚨 [USER_ABUSE] 유저 연사 전송 ({len(batch_data)}건)")
                
                elif mode == "PRODUCT_BURST":
                    batch_data = gen.generate_product_burst(count=BURST_COUNT)
                    print(f"🔥 [PRODUCT_BURST] 상품 폭주 전송 ({len(batch_data)}건)")

                # --- Kafka 전송 로직 ---
                for msg in batch_data:
                    producer.send(TOPIC_NAME, value=msg)
                    
                    # 💡 메시지 간 간격 (대량 테스트 시 주석 처리하거나 0으로 설정)
                    if len(batch_data) > 1:
                        time.sleep(BURST_INTERVAL) 
                
                producer.flush() # 전송 확정
                time.sleep(SCENARIO_DELAY)

    except KeyboardInterrupt:
        print("\n🛑 중단됨: 프로듀서를 종료합니다.")
        producer.close()