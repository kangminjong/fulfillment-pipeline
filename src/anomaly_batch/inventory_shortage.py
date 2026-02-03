"""
inventory_shortage.py
- "재고 부족" 이상탐지 로직만 담당하는 파일

아이디어(배치용):
- 최근 N분(=window_start~now) 동안 업데이트된 orders 중
- product_id가 있고
- products.stock <= 0 이면 "재고 부족"으로 판단

※ 왜 orders를 보냐?
- 이벤트(events)만 보는 것보다 "현재 주문 스냅샷(orders)"가
  product_id를 들고 있을 확률이 높아서 구현이 단순해짐.
"""

from typing import Dict, List


def detect_inventory_shortage(cur, window_start, window_end, eligible_statuses=None) -> List[Dict]:
    """
    재고 부족 탐지

    Args:
        cur: psycopg2 cursor
        window_start, window_end: 탐지 시간 범위 (timestamptz)
        eligible_statuses: 특정 status만 대상으로 할지(선택)
            - None이면 상태 상관없이 orders 갱신된 것 모두 검사
            - 예: ["ORDER_CREATED", "PAYMENT_COMPLETED"]

    Returns:
        alerts: 이상탐지 결과 리스트 (각 항목은 dict)
    """

    # 상태 필터를 쓸지 말지 결정
    # - 테스트/데모에서는 그냥 None으로 두고 넓게 잡아도 잘 돌아감
    status_filter_sql = ""
    params = {
        "window_start": window_start,
        "window_end": window_end,
    }

    if eligible_statuses:
        status_filter_sql = "AND o.current_status = ANY(%(eligible_statuses)s)"
        params["eligible_statuses"] = eligible_statuses

    # 핵심 쿼리:
    # - 최근 window 동안 updated_at이 갱신된 주문(orders)을 기준으로
    # - 상품(products)과 JOIN 해서 stock을 확인
    # - stock <= 0 이면 재고 부족으로 본다
    sql = f"""
    SELECT
        o.order_id,
        o.product_id,
        o.product_name,
        o.current_stage,
        o.current_status,
        p.stock
    FROM orders o
    JOIN products p
      ON o.product_id = p.product_id
    WHERE o.updated_at >= %(window_start)s
      AND o.updated_at <  %(window_end)s
      AND o.product_id IS NOT NULL
      AND p.stock <= 0
      {status_filter_sql}
    ORDER BY o.updated_at DESC;
    """

    cur.execute(sql, params)
    rows = cur.fetchall()

    alerts = []
    for (order_id, product_id, product_name, current_stage, current_status, stock) in rows:
        # anomaly_key는 "중복 알림 방지"에 사용됨
        # 같은 주문/상품에 대해 매 분마다 계속 알림 찍히는 걸 막기 위해 runner에서 활용
        anomaly_key = f"inventory_shortage|order:{order_id}|product:{product_id}"

        alerts.append(
            {
                "anomaly_type": "INVENTORY_SHORTAGE",
                "anomaly_key": anomaly_key,
                "order_id": order_id,
                "product_id": product_id,
                "product_name": product_name,
                "current_stage": current_stage,
                "current_status": current_status,
                "stock": stock,
                "message": "재고가 0 이하인데 주문이 진행 중입니다.",
            }
        )

    return alerts