import random
import string
from datetime import datetime
from faker import Faker
import json
fake = Faker('ko_KR')

class OrderGenerator:
    def __init__(self):
        self.product_catalog = self._init_products()
        self.product_ids = list(self.product_catalog.keys())

    def _init_products(self):
        products = {}
        categories = ["ELEC", "CLOTH", "FOOD", "BOOK", "TEST"]
        names = ["상품A", "상품B", "상품C", "상품D", "상품E", "상품F", "상품G", "상품H", "상품I", "상품J"]
        for cat in categories:
            for i, name in enumerate(names, 1):
                p_id = f"{cat}-{i:03d}"
                products[p_id] = f"{cat} {name}"
        return products

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

    # 개별 시나리오 데이터 생성 메서드들
    def generate_normal(self):
        return [self._base_data()]

    def generate_validation_error(self):
        data = self._base_data()
        data["order_id"] = "" # 테스트용 결함 주입
        return [data]

    def generate_out_of_stock(self):
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
    print(json.dumps(gen.generate_normal(), ensure_ascii=False, indent=2))