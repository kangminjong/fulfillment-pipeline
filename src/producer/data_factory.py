import random
import uuid
import json
from datetime import datetime, timedelta

# ---------------------------------------------------------
# ⚙️ 시나리오 설정 (테이블 상태값 정의)
# ---------------------------------------------------------
SCENARIOS = [
    # 1. 배송 중
    {
        'stage': 'SHIPMENT',
        'status': 'SHIPPED',
        'event_type': 'SHIPPED',
        'tracking': True,
        'date': True,
        'hold': False
    },
    # 2. 결제 완료
    {
        'stage': 'ORDER',
        'status': 'PAYMENT_CONFIRMED',
        'event_type': 'PAYMENT_CONFIRMED',
        'tracking': False,
        'date': False,
        'hold': False
    },
    # 3. 재고 부족 보류
    {
        'stage': 'ORDER',
        'status': 'HOLD',
        'event_type': 'HOLD',
        'tracking': False,
        'date': True,
        'hold': True
    }
]

def get_current_time_str():
    return datetime.now().isoformat()

def create_random_event():
    """
    [핵심] 껍데기 없이, 주문 테이블(order_current) 컬럼 그대로 JSON 생성
    """
    
    # 1. 시나리오 선택
    case = random.choice(SCENARIOS)
    
    # 2. 기본 데이터 생성 (SQL 테이블 컬럼명과 100% 일치)
    data = {
        "order_id": f"ORD-{uuid.uuid4()}",  # PK
        "current_stage": case['stage'],
        "current_status": case['status'],
        "last_event_type": case['event_type'],
        "last_occurred_at": get_current_time_str(),
        "updated_at": get_current_time_str(),
        
        # Nullable 컬럼들 (기본 None)
        "hold_reason_code": None,
        "tracking_no": None,
        "promised_delivery_date": None
    }

    # 3. 상황에 따라 빈칸 채우기
    if case['tracking']:
        data["tracking_no"] = f"{random.randint(1000,9999)}-{random.randint(1000,9999)}"
        
    if case['date']:
        data["promised_delivery_date"] = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
        
    if case['hold']:
        data["hold_reason_code"] = "HOLD_STOCKOUT"

    # 4. 그냥 이 딕셔너리(JSON) 자체를 리턴! (포장 X)
    return data

# ---------------------------------------------------------
# 🧪 확인용
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🏭 [데이터 공장] 주문 테이블 원본 데이터 생성 (3건)\n")
    for i in range(3):
        d = create_random_event()
        print(json.dumps(d, indent=2, ensure_ascii=False))
        print("-" * 50)