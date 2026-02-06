import time
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from src.producer.data_factory import OrderGenerator

load_dotenv()

# =========================================================
# ⚙️ 설정
# =========================================================
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_NAME = os.getenv("KAFKA_TOPIC")

# [중요] 탐지 임계치가 5회이므로, 8~10개를 보내야 확실하게 걸림
BURST_COUNT = 8  

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
        acks='all',
        linger_ms=5,      # 배치 전송 효율화
        batch_size=32768
    )

if __name__ == "__main__":
    gen = OrderGenerator()
    producer = create_producer()
    
    print(f"[Producer] Kafka: {BOOTSTRAP_SERVERS}")
    print(f"[Topic] {TOPIC_NAME}")
    print(f"[Mode] Past Data Simulation (과거 데이터 적재 모드)")
    print("-" * 50)

    # 시나리오 정의
    SCENARIOS = [
        "STOCK_OUT"
    ]

    try:
        loop_cnt = 0
        # while True:
        #     loop_cnt += 1
        #     print(f"\n[Loop {loop_cnt}] 데이터 생성 시작...")

        for mode in SCENARIOS:
                        batch_data = []
                        
                        # Factory에서 데이터 생성 (과거 시간 포함)
                        if mode == "NORMAL":
                            batch_data = gen.generate_normal()
                        elif mode == "VALID_ERROR":
                            batch_data = gen.generate_validation_error()
                        elif mode == "STOCK_OUT":
                            batch_data = gen.generate_out_of_stock()
                        elif mode == "USER_BURST":
                            batch_data = gen.generate_user_burst(BURST_COUNT)
                        elif mode == "PROD_BURST":
                            batch_data = gen.generate_product_burst(BURST_COUNT)
                        elif mode == "EMPTY_JSON":
                            batch_data = gen.generate_empty_json()
                        # 전송 로직
                        for msg in batch_data:
                            # Burst 모드일 때는 딜레이 없이 쏴야 Consumer가 
                            # "이 데이터들이 동시에 왔구나"라고 판단하기 좋음
                            if mode in ["USER_BURST", "PROD_BURST"]:
                                producer.send(TOPIC_NAME, value=msg)
                            else:
                                producer.send(TOPIC_NAME, value=msg)
                                time.sleep(0.1) # 정상 데이터는 약간의 텀

                        producer.flush()
                        time.sleep(1.0) # 시나리오 구분용 텀

    except KeyboardInterrupt:
        print("\n테스트 종료")
        producer.close()