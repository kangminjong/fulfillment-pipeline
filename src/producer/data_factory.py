import random
import string
import json
from datetime import datetime
from faker import Faker

fake = Faker('ko_KR')

class OrderGenerator:
    def __init__(self):
        # SQL 데이터를 바탕으로 실제 상품 카탈로그 구축
        self.product_catalog = self._init_products()
        self.product_ids = list(self.product_catalog.keys())

    def _init_products(self):
        # 제공해주신 SQL 데이터를 딕셔너리로 변환
        return {
            # 1. 💻 전자제품
            'ELEC-001': '맥북 프로 16인치 M3', 'ELEC-002': '갤럭시북4 울트라',
            'ELEC-003': '아이패드 에어 6세대', 'ELEC-004': '소니 노이즈캔슬링 헤드폰 XM5',
            'ELEC-005': 'LG 울트라기어 32인치 모니터', 'ELEC-006': '로지텍 MX Master 3S 마우스',
            'ELEC-007': '기계식 키보드 (적축)', 'ELEC-008': 'C타입 고속 충전기 65W',
            'ELEC-009': 'HDMI 2.1 케이블', 'ELEC-010': '스마트폰 짐벌 안정기',

            # 2. 👕 의류/패션
            'CLOTH-001': '남성용 기본 무지 티셔츠 (L)', 'CLOTH-002': '남성용 기본 무지 티셔츠 (XL)',
            'CLOTH-003': '여성용 슬림핏 청바지 (27)', 'CLOTH-004': '여성용 슬림핏 청바지 (28)',
            'CLOTH-005': '유니섹스 후드 집업 (Grey)', 'CLOTH-006': '스포츠 러닝 양말 3팩',
            'CLOTH-007': '방수 윈드브레이커 자켓', 'CLOTH-008': '캔버스 에코백 (Ivory)',
            'CLOTH-009': '베이스볼 캡 모자 (Black)', 'CLOTH-010': '겨울용 스마트폰 터치 장갑',

            # 3. 🍎 식품/생필품
            'FOOD-001': '제주 삼다수 2L x 6개입', 'FOOD-002': '신라면 멀티팩 (5개입)',
            'FOOD-003': '햇반 210g x 12개입', 'FOOD-004': '서울우유 1L',
            'FOOD-005': '유기농 바나나 1송이', 'FOOD-006': '냉동 닭가슴살 1kg',
            'FOOD-007': '맥심 모카골드 믹스커피 100T', 'FOOD-008': '3겹 데코 롤휴지 30롤',
            'FOOD-009': '물티슈 100매 캡형', 'FOOD-010': 'KF94 마스크 대형 50매',

            # 4. 📚 도서/취미
            'BOOK-001': '데이터 엔지니어링 교과서', 'BOOK-002': '파이썬으로 시작하는 데이터 분석',
            'BOOK-003': 'SQL 레벨업 가이드', 'BOOK-004': '해리포터 전집 세트',
            'BOOK-005': '닌텐도 스위치 OLED 게임기', 'BOOK-006': '젤다의 전설 게임 타이틀',
            'BOOK-007': '건담 프라모델 (MG 등급)', 'BOOK-008': '전문가용 48색 색연필',
            'BOOK-009': '요가 매트 (10mm)', 'BOOK-010': '캠핑용 접이식 의자',

            # 5. 🚨 테스트용 상품
            'TEST-001': '한정판 스니커즈 (품절임박)', 'TEST-002': '인기 아이돌 앨범 (재고부족)',
            'TEST-003': '단종된 레거시 상품', 'TEST-004': '이벤트 경품 (선착순)',
            'TEST-005': '창고 깊숙한 곳 악성재고', 'TEST-006': '시스템 오류 유발 상품 A',
            'TEST-007': '시스템 오류 유발 상품 B', 'TEST-008': '배송 지연 예상 상품',
            'TEST-009': '합포장 테스트용 상품 A', 'TEST-010': '합포장 테스트용 상품 B'
        }

    def _generate_order_id(self, product_id):
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
        return f"ord-{now}-{product_id}-{suffix}"

    def _base_data(self, user_id=None, product_id=None):
        p_id = product_id if product_id else random.choice(self.product_ids)
        return {
            "order_id": self._generate_order_id(p_id),
            "user_id": user_id if user_id is not None else fake.user_name(),
            "product_id": p_id,
            "product_name": self.product_catalog.get(p_id, "알 수 없는 상품"),
            "shipping_address": fake.address(),
            "event_time": datetime.now().isoformat()
        }

    def generate_normal(self):
        return [self._base_data()]

    def generate_validation_error(self):
        data = self._base_data()
        data["order_id"] = "" # [에러 유도] PK 누락
        return [data]

    def generate_out_of_stock(self):
        # 실제 SQL의 TEST-002 상품 사용
        return [self._base_data(product_id="TEST-002")]

    def generate_user_burst(self, count):
        u_id = fake.user_name()
        return [self._base_data(user_id=u_id) for _ in range(count)]

    def generate_product_burst(self, count):
        p_id = random.choice(self.product_ids)
        return [self._base_data(product_id=p_id) for _ in range(count)]

# 실행 확인용
if __name__ == "__main__":
    gen = OrderGenerator()
    print("--- 실제 데이터 샘플 출력 ---")
    print(json.dumps(gen.generate_normal(), ensure_ascii=False, indent=2))