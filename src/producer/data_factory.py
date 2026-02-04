import random
import string
import json
import uuid
from datetime import datetime
from faker import Faker

# 한국어 더미 데이터 생성기 초기화
fake = Faker('ko_KR')

class OrderGenerator:
    def __init__(self):
        # 1. 50종의 상품 카탈로그 구축
        self.product_catalog = self._init_products()
        self.product_ids = list(self.product_catalog.keys())
        
        # 2. 이미지 기반 상태(Status)와 단계(Stage) 매핑 로직
        # 상태에 따라 어느 파이프라인 단계에 있는지 정의합니다.
        self.status_stage_map = {
            "PAID": "PAYMENT",            # 결제완료 -> 결제 단계
            "PICKING": "FULFILLMENT",     # 피킹중 -> 풀필먼트 단계
            "PACKED": "FULFILLMENT",      # 포장완료 -> 풀필먼트 단계
            "SHIPPED": "LOGISTICS",       # 출고/발송완료 -> 배송 단계
            "DELIVERED": "LOGISTICS",     # 배송완료 -> 배송 단계
            "CANCELED": "SYSTEM",         # 취소 -> 시스템 처리
            "HOLD": "RISK_CHECK"          # 보류 -> 리스크 검사 단계
        }

    def _init_products(self):
        """제공해주신 SQL 데이터를 바탕으로 구축한 전체 상품 카탈로그"""
        return {
            # 1. 💻 전자제품 (Electronics)
            'ELEC-001': '맥북 프로 16인치 M3', 'ELEC-002': '갤럭시북4 울트라',
            'ELEC-003': '아이패드 에어 6세대', 'ELEC-004': '소니 노이즈캔슬링 헤드폰 XM5',
            'ELEC-005': 'LG 울트라기어 32인치 모니터', 'ELEC-006': '로지텍 MX Master 3S 마우스',
            'ELEC-007': '기계식 키보드 (적축)', 'ELEC-008': 'C타입 고속 충전기 65W',
            'ELEC-009': 'HDMI 2.1 케이블', 'ELEC-010': '스마트폰 짐벌 안정기',

            # 2. 👕 의류/패션 (Clothing)
            'CLOTH-001': '남성용 기본 무지 티셔츠 (L)', 'CLOTH-002': '남성용 기본 무지 티셔츠 (XL)',
            'CLOTH-003': '여성용 슬림핏 청바지 (27)', 'CLOTH-004': '여성용 슬림핏 청바지 (28)',
            'CLOTH-005': '유니섹스 후드 집업 (Grey)', 'CLOTH-006': '스포츠 러닝 양말 3팩',
            'CLOTH-007': '방수 윈드브레이커 자켓', 'CLOTH-008': '캔버스 에코백 (Ivory)',
            'CLOTH-009': '베이스볼 캡 모자 (Black)', 'CLOTH-010': '겨울용 스마트폰 터치 장갑',

            # 3. 🍎 식품/생필품 (Food & Essentials)
            'FOOD-001': '제주 삼다수 2L x 6개입', 'FOOD-002': '신라면 멀티팩 (5개입)',
            'FOOD-003': '햇반 210g x 12개입', 'FOOD-004': '서울우유 1L',
            'FOOD-005': '유기농 바나나 1송이', 'FOOD-006': '냉동 닭가슴살 1kg',
            'FOOD-007': '맥심 모카골드 믹스커피 100T', 'FOOD-008': '3겹 데코 롤휴지 30롤',
            'FOOD-009': '물티슈 100매 캡형', 'FOOD-010': 'KF94 마스크 대형 50매',

            # 4. 📚 도서/취미 (Books & Hobbies)
            'BOOK-001': '데이터 엔지니어링 교과서', 'BOOK-002': '파이썬으로 시작하는 데이터 분석',
            'BOOK-003': 'SQL 레벨업 가이드', 'BOOK-004': '해리포터 전집 세트',
            'BOOK-005': '닌텐도 스위치 OLED 게임기', 'BOOK-006': '젤다의 전설 게임 타이틀',
            'BOOK-007': '건담 프라모델 (MG 등급)', 'BOOK-008': '전문가용 48색 색연필',
            'BOOK-009': '요가 매트 (10mm)', 'BOOK-010': '캠핑용 접이식 의자',

            # 5. 🚨 테스트용 상품 (Special/Test)
            'TEST-001': '한정판 스니커즈 (품절임박)', 'TEST-002': '인기 아이돌 앨범 (재고부족)',
            'TEST-003': '단종된 레거시 상품', 'TEST-004': '이벤트 경품 (선착순)',
            'TEST-005': '창고 깊숙한 곳 악성재고', 'TEST-006': '시스템 오류 유발 상품 A',
            'TEST-007': '시스템 오류 유발 상품 B', 'TEST-008': '배송 지연 예상 상품',
            'TEST-009': '합포장 테스트용 상품 A', 'TEST-010': '합포장 테스트용 상품 B'
        }

    def _generate_order_id(self, product_id):
        """주문 ID 생성: ord-날짜-상품ID-랜덤"""
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
        return f"ord-{now}-{product_id}-{suffix}"

    def _base_data(self, user_id=None, product_id=None):
        """단일 주문 데이터 생성 (핵심 로직)"""
        p_id = product_id if product_id else random.choice(self.product_ids)
        
        # 💡 이미지의 7가지 상태 중 하나를 랜덤 선택
        random_status = random.choice(list(self.status_stage_map.keys()))
        # 💡 선택된 상태에 어울리는 단계를 자동으로 매핑
        corresponding_stage = self.status_stage_map[random_status]
        
        return {
            "order_id": self._generate_order_id(p_id),
            "user_id": user_id if user_id is not None else fake.user_name(), # DB 매핑: customer_id -> user_id
            "product_id": p_id,
            "product_name": self.product_catalog.get(p_id, "알 수 없는 상품"),
            "shipping_address": fake.address(),                              # DB 매핑: address -> shipping_address
            
            # 💡 매핑된 상태와 단계
            "current_status": random_status,
            "current_stage": corresponding_stage,
            
            "last_event_type": "ORDER_CREATED",
            "last_occurred_at": datetime.now().isoformat()
        }

    # ---------------------------------------------------------
    # 🧪 시나리오별 데이터 생성 메서드
    # ---------------------------------------------------------
    def generate_normal(self):
        """정상 주문 1건 생성 (랜덤 상태 포함)"""
        return [self._base_data()]

    def generate_validation_error(self):
        """필수 정보 누락 에러 시나리오 (user_id 또는 address 누락)"""
        data = self._base_data()
        targets = ["user_id", "shipping_address"]
        # 1개 또는 2개 필드를 랜덤하게 비움
        nuke_fields = random.sample(targets, random.randint(1, len(targets)))
        for field in nuke_fields:
            data[field] = ""
        return [data]

    def generate_out_of_stock(self):
        """재고 부족 시나리오 (TEST-002 상품 고정)"""
        return [self._base_data(product_id="TEST-002")]

    def generate_user_burst(self, count):
        u_id = fake.user_name()
        p_id = random.choice(self.product_ids) 
        return [self._base_data(user_id=u_id, product_id=p_id) for _ in range(count)]

    def generate_product_burst(self, count):
        p_id = random.choice(self.product_ids)
        batch = []
        for _ in range(count):
            data = self._base_data(product_id=p_id)
            data.update({"current_status": "PAID", "current_stage": "PAYMENT"})
            batch.append(data)
        return batch

# ---------------------------------------------------------
# 🚀 테스트 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    gen = OrderGenerator()
    print("--- 🛒 생성된 샘플 데이터 (랜덤 상태/단계 적용) ---")
    print(json.dumps(gen.generate_normal(), ensure_ascii=False, indent=2))