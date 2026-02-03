import time
import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker

# ---------------------------------------------------------
# 1. 설정 및 라이브러리 초기화
# ---------------------------------------------------------
fake = Faker('ko_KR')  # 한국어 더미 데이터 생성기

# ---------------------------------------------------------
# 2. 상품 전체 리스트 (50개) - DB와 ID 일치
# ---------------------------------------------------------
FULL_PRODUCT_CATALOG = [
    # 1. 전자제품
    {"id": "ELEC-001", "name": "맥북 프로 16인치 M3"},
    {"id": "ELEC-002", "name": "갤럭시북4 울트라"},
    {"id": "ELEC-003", "name": "아이패드 에어 6세대"},
    {"id": "ELEC-004", "name": "소니 노이즈캔슬링 헤드폰 XM5"},
    {"id": "ELEC-005", "name": "LG 울트라기어 32인치 모니터"},
    {"id": "ELEC-006", "name": "로지텍 MX Master 3S 마우스"},
    {"id": "ELEC-007", "name": "기계식 키보드 (적축)"},
    {"id": "ELEC-008", "name": "C타입 고속 충전기 65W"},
    {"id": "ELEC-009", "name": "HDMI 2.1 케이블"},
    {"id": "ELEC-010", "name": "스마트폰 짐벌 안정기"},
    # 2. 의류
    {"id": "CLOTH-001", "name": "남성용 기본 무지 티셔츠 (L)"},
    {"id": "CLOTH-002", "name": "남성용 기본 무지 티셔츠 (XL)"},
    {"id": "CLOTH-003", "name": "여성용 슬림핏 청바지 (27)"},
    {"id": "CLOTH-004", "name": "여성용 슬림핏 청바지 (28)"},
    {"id": "CLOTH-005", "name": "유니섹스 후드 집업 (Grey)"},
    {"id": "CLOTH-006", "name": "스포츠 러닝 양말 3팩"},
    {"id": "CLOTH-007", "name": "방수 윈드브레이커 자켓"},
    {"id": "CLOTH-008", "name": "캔버스 에코백 (Ivory)"},
    {"id": "CLOTH-009", "name": "베이스볼 캡 모자 (Black)"},
    {"id": "CLOTH-010", "name": "겨울용 스마트폰 터치 장갑"},
    # 3. 식품/생필품
    {"id": "FOOD-001", "name": "제주 삼다수 2L x 6개입"},
    {"id": "FOOD-002", "name": "신라면 멀티팩 (5개입)"},
    {"id": "FOOD-003", "name": "햇반 210g x 12개입"},
    {"id": "FOOD-004", "name": "서울우유 1L"},
    {"id": "FOOD-005", "name": "유기농 바나나 1송이"},
    {"id": "FOOD-006", "name": "냉동 닭가슴살 1kg"},
    {"id": "FOOD-007", "name": "맥심 모카골드 믹스커피 100T"},
    {"id": "FOOD-008", "name": "3겹 데코 롤휴지 30롤"},
    {"id": "FOOD-009", "name": "물티슈 100매 캡형"},
    {"id": "FOOD-010", "name": "KF94 마스크 대형 50매"},
    # 4. 도서/취미
    {"id": "BOOK-001", "name": "데이터 엔지니어링 교과서"},
    {"id": "BOOK-002", "name": "파이썬으로 시작하는 데이터 분석"},
    {"id": "BOOK-003", "name": "SQL 레벨업 가이드"},
    {"id": "BOOK-004", "name": "해리포터 전집 세트"},
    {"id": "BOOK-005", "name": "닌텐도 스위치 OLED 게임기"},
    {"id": "BOOK-006", "name": "젤다의 전설 게임 타이틀"},
    {"id": "BOOK-007", "name": "건담 프라모델 (MG 등급)"},
    {"id": "BOOK-008", "name": "전문가용 48색 색연필"},
    {"id": "BOOK-009", "name": "요가 매트 (10mm)"},
    {"id": "BOOK-010", "name": "캠핑용 접이식 의자"},
    # 5. 🚨 테스트용 (함정 데이터용)
    {"id": "TEST-001", "name": "한정판 스니커즈 (품절임박)"},
    {"id": "TEST-002", "name": "인기 아이돌 앨범 (재고부족)"},
    {"id": "TEST-003", "name": "단종된 레거시 상품"},
    {"id": "TEST-004", "name": "이벤트 경품 (선착순)"},
    {"id": "TEST-005", "name": "창고 깊숙한 곳 악성재고"},
]

# ---------------------------------------------------------
# 3. 상태 리스트 (HOLD 없음!)
# : Producer는 무조건 정상적인 상태값만 보냅니다.
# ---------------------------------------------------------
STATUS_OPTS = [
    {"stage": "ORDER",       "status": "CREATED",           "event": "ORDER_CREATED"},
    {"stage": "ORDER",       "status": "PAID",              "event": "PAYMENT_CONFIRMED"}, # 👈 주로 이게 함정 카드로 쓰임
    {"stage": "FULFILLMENT", "status": "PICKING",           "event": "PICKING_STARTED"},
    {"stage": "FULFILLMENT", "status": "PACKED",            "event": "PACKING_COMPLETED"},
    {"stage": "SHIPMENT",    "status": "SHIPPED",           "event": "SHIPMENT_STARTED"},
    {"stage": "SHIPMENT",    "status": "DELIVERED",         "event": "DELIVERY_COMPLETED"},
    {"stage": "ORDER",       "status": "CANCELED",          "event": "ORDER_CANCELED"},
]

def get_now_str():
    return datetime.now().isoformat()

def get_random_address(is_broken=False):
    if is_broken:
        # Consumer가 잡아야 할 '쓰레기 데이터'
        return random.choice([None, "", "...", "???", "Unknown", "NULL", "123", "Seoul Only"])
    else:
        # 정상 주소
        return fake.address()

def create_raw_data():
    """
    Consumer가 '검증'할 수 있도록 원석(Raw) 데이터를 생성
    - 상태(Status)는 정상이지만, 내용(Content)이 불량인 데이터를 섞어 보냄
    """
    
    # 1. 상태 랜덤 선택 (Weights: 정상 흐름 위주)
    status_cfg = random.choices(STATUS_OPTS, k=1)[0]
    
    # 2. 기본 데이터 설정 (정상 가정)
    product = random.choice(FULL_PRODUCT_CATALOG)
    customer_id = customer_id = f"{fake.user_name()}{random.randint(100, 9999)}"
    address = get_random_address(is_broken=False)
    
    # -------------------------------------------------------
    # 💣 함정 심기 (Data Injection) - 20% 확률
    # : 상태는 바꾸지 않고, 데이터 내용물만 망가뜨림
    # -------------------------------------------------------
    is_trap = random.random() < 0.20

    if is_trap:
        # 세 가지 시나리오 중 하나 랜덤 선택
        trap_type = random.choice(['STOCKOUT', 'BAD_ADDR', 'FRAUD'])
        
        if trap_type == 'STOCKOUT':
            # [시나리오 1] 재고 부족
            # 상태는 'PAID' 등으로 나가지만, 상품은 '재고 없는 놈(TEST-002)'을 보냄
            product = {"id": "TEST-002", "name": "인기 아이돌 앨범 (재고부족)"}
            
        elif trap_type == 'BAD_ADDR':
            # [시나리오 2] 주소 오류
            # 상태는 정상이지만, 주소가 깨져있음
            address = get_random_address(is_broken=True)
            
        elif trap_type == 'FRAUD':
            # [시나리오 3] 이상 거래
            # 블랙리스트 유저 ID 사용
            customer_id = "black_consumer_001"

    # -------------------------------------------------------

    # 3. 데이터 조립 (SQL 스키마 100% 매칭)
    # : 송장번호나 HOLD 사유는 Producer가 알 수 없으므로 None 처리
    data = {
        "order_id": f"ORD-{uuid.uuid4()}",
        
        # [상품 정보]
        "product_id": product["id"],
        "product_name": product["name"],
        
        # [상태 정보] - 함정이 있어도 상태는 정상인 척 보냄!
        "current_stage": status_cfg["stage"],
        "current_status": status_cfg["status"],
        "last_event_type": status_cfg["event"],
        
        # [시간 정보]
        "last_occurred_at": get_now_str(),
        "updated_at": get_now_str(),
        "promised_delivery_date": (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d'),
        
        # [고객 정보]
        "customer_id": customer_id,
        "address": address,
        
        # [Producer가 모르는 필드 - Consumer가 채울 영역]
        "tracking_no": None,
        "hold_reason_code": None, # Producer는 이유를 모름
        "hold_ops_status": None,
        "hold_ops_note": None,
        "hold_ops_operator": None,
        "hold_ops_updated_at": None
    }
    
    return data

# ---------------------------------------------------------
# 🚀 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        data = create_raw_data()
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except KeyboardInterrupt:
        print("\n🛑 생성 종료")