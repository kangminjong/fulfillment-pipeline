import random
import string
import json
import uuid
from datetime import datetime, timezone, timedelta
from faker import Faker

# 한국어 더미 데이터 생성기 초기화
fake = Faker('ko_KR')
KST = timezone(timedelta(hours=9))

class OrderGenerator:
    def __init__(self):
        # 1. 50종의 상품 카탈로그 구축
        self.product_catalog = self._init_products()
        self.product_ids = list(self.product_catalog.keys())
        
        # 2. 상태(Status)와 단계(Stage) 매핑
        self.status_stage_map = {
            "PAID": "PAYMENT",
            "PICKING": "FULFILLMENT",
            "PACKED": "FULFILLMENT",
            "SHIPPED": "LOGISTICS",
            "DELIVERED": "LOGISTICS",
            "CANCELED": "SYSTEM",
        }

    def _init_products(self):
        return {
            'ELEC-001': '맥북 프로 16인치 M3', 'ELEC-002': '갤럭시북4 울트라',
            'ELEC-003': '아이패드 에어 6세대', 'ELEC-004': '소니 노이즈캔슬링 헤드폰 XM5',
            'ELEC-005': 'LG 울트라기어 32인치 모니터', 'ELEC-006': '로지텍 MX Master 3S 마우스',
            'ELEC-007': '기계식 키보드 (적축)', 'ELEC-008': 'C타입 고속 충전기 65W',
            'ELEC-009': 'HDMI 2.1 케이블', 'ELEC-010': '스마트폰 짐벌 안정기',
            'CLOTH-001': '남성용 기본 무지 티셔츠 (L)', 'CLOTH-002': '남성용 기본 무지 티셔츠 (XL)',
            'CLOTH-003': '여성용 슬림핏 청바지 (27)', 'CLOTH-004': '여성용 슬림핏 청바지 (28)',
            'CLOTH-005': '유니섹스 후드 집업 (Grey)', 'CLOTH-006': '스포츠 러닝 양말 3팩',
            'CLOTH-007': '방수 윈드브레이커 자켓', 'CLOTH-008': '캔버스 에코백 (Ivory)',
            'CLOTH-009': '베이스볼 캡 모자 (Black)', 'CLOTH-010': '겨울용 스마트폰 터치 장갑',
            'FOOD-001': '제주 삼다수 2L x 6개입', 'FOOD-002': '신라면 멀티팩 (5개입)',
            'FOOD-003': '햇반 210g x 12개입', 'FOOD-004': '서울우유 1L',
            'FOOD-005': '유기농 바나나 1송이', 'FOOD-006': '냉동 닭가슴살 1kg',
            'FOOD-007': '맥심 모카골드 믹스커피 100T', 'FOOD-008': '3겹 데코 롤휴지 30롤',
            'FOOD-009': '물티슈 100매 캡형', 'FOOD-010': 'KF94 마스크 대형 50매',
            'BOOK-001': '데이터 엔지니어링 교과서', 'BOOK-002': '파이썬으로 시작하는 데이터 분석',
            'BOOK-003': 'SQL 레벨업 가이드', 'BOOK-004': '해리포터 전집 세트',
            'BOOK-005': '닌텐도 스위치 OLED 게임기', 'BOOK-006': '젤다의 전설 게임 타이틀',
            'BOOK-007': '건담 프라모델 (MG 등급)', 'BOOK-008': '전문가용 48색 색연필',
            'BOOK-009': '요가 매트 (10mm)', 'BOOK-010': '캠핑용 접이식 의자',
            'TEST-001': '한정판 스니커즈 (품절임박)', 'TEST-002': '인기 아이돌 앨범 (재고부족)',
            'TEST-003': '단종된 레거시 상품', 'TEST-004': '이벤트 경품 (선착순)',
            'TEST-005': '창고 깊숙한 곳 악성재고', 'TEST-006': '시스템 오류 유발 상품 A',
            'TEST-007': '시스템 오류 유발 상품 B', 'TEST-008': '배송 지연 예상 상품',
            'TEST-009': '합포장 테스트용 상품 A', 'TEST-010': '합포장 테스트용 상품 B'
        }
    def _get_current_time(self):
        """시스템 로컬 시간과 상관없이 정확한 한국 시각을 반환"""
        # UTC 기준 시간을 먼저 가져온 뒤 KST로 강제 변환합니다.
        return datetime.now(timezone.utc).astimezone(KST)

    def _get_random_past_time(self):
        """2026년 1월 20일 ~ 현재(KST) 사이의 랜덤 시간 생성"""
        # 현재 시각을 UTC 기반 KST로 가져와서 미래로 가는 것을 방지합니다.
        now = datetime.now(timezone.utc).astimezone(KST) 
        
        start_date = datetime(2026, 1, 20, tzinfo=KST)
        
        # 만약 시작일이 지금보다 미래라면 (그럴 리 없겠지만) 현재 시각 반환
        if start_date > now: 
            return now
            
        time_diff = now - start_date
        random_seconds = random.randint(0, int(time_diff.total_seconds()))
        
        return start_date + timedelta(seconds=random_seconds)

    def _generate_order_id(self, product_id, date_obj):
        """주문 ID 생성"""
        date_str = date_obj.strftime("%Y%m%d%H%M%S")
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
        return f"ord-{date_str}-{product_id}-{suffix}"

    # 🚨 [수정 핵심] fixed_t0 파라미터 추가
    # 폭주 시나리오에서는 이 파라미터로 시간을 고정해서 넘겨줍니다.
    def _base_data(self, user_id=None, product_id=None, fixed_t0=None):
        """단일 주문 데이터 생성 (타임머신 로직 적용)"""
        p_id = product_id if product_id else random.choice(self.product_ids)
        
        # 1. 📅 [T0] 시간 결정
        # 고정된 시간(fixed_t0)이 들어오면 그걸 쓰고, 아니면 랜덤 생성
        if fixed_t0:
            t0_past = fixed_t0
        else:
            t0_past = self._get_current_time()
        
        # 2. 📅 [T2] DB 저장 시간 (T0보다 0.1~1.5초 뒤)
        simulated_delay = random.uniform(0.1, 1.5)
        t2_past = t0_past + timedelta(seconds=simulated_delay)
        
        t0_str = t0_past.isoformat()
        t2_str = t2_past.isoformat()
        
        random_status = random.choice(list(self.status_stage_map.keys()))
        corresponding_stage = self.status_stage_map[random_status]
        
        return {
            "order_id": self._generate_order_id(p_id, t0_past),
            "user_id": user_id if user_id is not None else fake.user_name(),
            "product_id": p_id,
            "product_name": self.product_catalog.get(p_id, "알 수 없는 상품"),
            "shipping_address": fake.address(),
            "current_status": random_status,
            "current_stage": corresponding_stage,
            "event_produced_at": t0_str,
            "created_at": t2_str,
            "last_occurred_at": t2_str,
            "occurred_at": t0_str,
            "last_event_type": "ORDER_CREATED"
        }

    # ---------------------------------------------------------
    # 🧪 시나리오 메서드 (수정됨)
    # ---------------------------------------------------------
    def generate_normal(self):
        return [self._base_data()]

    def generate_validation_error(self):
        data = self._base_data()
        targets = ["user_id", "shipping_address"]
        nuke_fields = random.sample(targets, random.randint(1, len(targets)))
        for field in nuke_fields:
            data[field] = ""
        return [data]

    def generate_out_of_stock(self):
        return [self._base_data(product_id="TEST-002")]

    # 🚨 [수정] 유저 도배: 기준 시간을 잡고 0.1초씩만 증가시킴
    def generate_user_burst(self, count):
        u_id = fake.user_name()
        p_id = random.choice(self.product_ids)
        
        # 기준 시간(Base Time) 하나 생성
        base_time = self._get_current_time()
        
        batch = []
        for i in range(count):
            # 0.5초 간격으로 생성 (10초 윈도우 안에 충분히 들어옴)
            current_t0 = base_time + timedelta(seconds=i * 0.5)
            batch.append(self._base_data(user_id=u_id, product_id=p_id, fixed_t0=current_t0))
        return batch

    # 🚨 [수정] 상품 폭주: 기준 시간을 잡고 0.05초씩만 증가시킴 (1초 윈도우라 더 촘촘하게)
    def generate_product_burst(self, count):
        p_id = random.choice(self.product_ids)
        
        # 기준 시간 생성
        base_time = self._get_current_time()
        
        batch = []
        for i in range(count):
            # 0.05초 간격으로 생성 (1초 윈도우 안에 들어오도록)
            current_t0 = base_time + timedelta(seconds=i * 0.05)
            
            data = self._base_data(product_id=p_id, fixed_t0=current_t0)
            data.update({"current_status": "PAID", "current_stage": "PAYMENT"})
            batch.append(data)
        return batch
    
    def generate_empty_json(self):
        """
        [시나리오] JSON은 맞는데 내용이 텅 빈 경우 ({})
        Consumer의 'if not data:' 방어 로직 테스트용
        """
        return [{}]
# ---------------------------------------------------------
# 🚀 테스트 실행부
# ---------------------------------------------------------
if __name__ == "__main__":
    gen = OrderGenerator()
    
    print("\n--- ☠️ [검증 3] Empty JSON 생성 확인 ---")
    empty_data = gen.generate_normal()
    print(f"생성된 데이터: {empty_data}")