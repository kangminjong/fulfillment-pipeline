import time
import json
import os
import random
from kafka import KafkaProducer

# 🚨 [중요] data_factory.py가 같은 폴더에 있어야 하며, 
# OrderGenerator 클래스 안에 generate_empty_json() 메서드가 있어야 합니다.
try:
    from data_factory import OrderGenerator
except ImportError:
    from src.producer.data_factory import OrderGenerator

# ---------------------------------------------------------
# ⚙️ 설정 및 속도 제어
# ---------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'event'

# [속도 조절] 시뮬레이션의 리듬을 조절하세요
SCENARIO_DELAY = 2.0   # 시나리오 하나 끝나고 쉴 시간 (초) - Consumer가 처리할 여유를 줌
BURST_COUNT = 6        # 도배/폭주 시 생성할 주문 개수 (5개 초과여야 HOLD 됨)
BURST_INTERVAL = 0.05  # 버스트 시 메시지 간 간격 (빠르게 쏴야 함)

def create_producer():
    try:
        # ✅ 빈 딕셔너리({})는 JSON 직렬화에 문제 없으므로 기본 serializer 사용
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            acks=1,
            linger_ms=10
        )
        print(f"✅ 카프카 연결 성공! ({BOOTSTRAP_SERVERS})")
        return producer
    except Exception as e:
        print(f"❌ 카프카 연결 실패: {e}")
        return None

if __name__ == "__main__":
    gen = OrderGenerator()
    producer = create_producer()
    
    if not producer:
        exit(1)

    # ✅ [수정] 실행할 시나리오 순서 (콤마 오류 수정됨)
    SCENARIO_SEQUENCE = [
        "NORMAL",         # 정상 데이터
        "CHAOS_EMPTY",    # ☠️ 빈 JSON({}) 공격 -> Consumer 방어 테스트
        "VALID_ERROR",    # 필수 정보 누락
        "OUT_OF_STOCK",   # 재고 부족
        "USER_ABUSE",     # 유저 도배
        "PRODUCT_BURST",  # 상품 폭주
    ]

    print(f"🚀 [프로듀서] 타임머신 시뮬레이션 시작! (토픽: {TOPIC_NAME})")
    print(f"📋 시나리오 순서: {SCENARIO_SEQUENCE}")
    print("-" * 50)

    try:
        # 무한 루프로 계속 데이터를 생성해 DB를 채웁니다.
        while True:
            for mode in SCENARIO_SEQUENCE:
                batch_data = []

                # --- [시나리오별 데이터 생성] ---
                if mode == "NORMAL":
                    # 정상 주문 1~2건 랜덤 생성
                    for _ in range(random.randint(1, 2)):
                        batch_data.extend(gen.generate_normal())
                    print(f"📦 [NORMAL] 정상 주문 전송")

                elif mode == "CHAOS_EMPTY":
                    # Consumer의 빈 값 방어 로직 테스트용
                    batch_data = gen.generate_empty_json() 
                    print(f"💣 [CHAOS_EMPTY] 빈 JSON({{}}) 투척 (Skip 확인용)")
                
                elif mode == "VALID_ERROR":
                    batch_data = gen.generate_validation_error()
                    print(f"⚠️ [VALID_ERROR] 결함 데이터(주소/ID 누락) 전송")
                
                elif mode == "OUT_OF_STOCK":
                    batch_data = gen.generate_out_of_stock()
                    print(f"🚫 [OUT_OF_STOCK] 재고 부족 상품(TEST-002) 전송")
                
                elif mode == "USER_ABUSE":
                    batch_data = gen.generate_user_burst(count=BURST_COUNT)
                    print(f"🔥 [USER_ABUSE] 유저 도배 시뮬레이션 ({len(batch_data)}건) 전송")
                
                elif mode == "PRODUCT_BURST":
                    batch_data = gen.generate_product_burst(count=BURST_COUNT)
                    print(f"📈 [PRODUCT_BURST] 상품 주문 폭주 ({len(batch_data)}건) 전송")

                # --- [Kafka 전송] ---
                for msg in batch_data:
                    producer.send(TOPIC_NAME, value=msg)
                    
                    # 버스트 모드일 때도 순서 보장을 위해 아주 미세한 텀을 둠
                    if len(batch_data) > 1:
                        time.sleep(BURST_INTERVAL)
                
                producer.flush() # 즉시 전송
                
                # 시나리오 사이 대기
                time.sleep(SCENARIO_DELAY)

            print("🔄 한 사이클 완료. 잠시 후 다시 시작합니다...")
            print("-" * 30)

    except KeyboardInterrupt:
        print("\n🛑 시뮬레이션 중단 요청. 프로듀서를 종료합니다.")
        producer.close()