"""
inventory_shortage.py
- 최근 window 동안 갱신된 주문 중
  products.stock <= 0 인 상품이 있으면 '재고 부족' 이상으로 탐지

DB 기준 (DB.png):
- public.orders: order_id, product_id, current_status, updated_at ...
- public.products: product_id, stock ...
"""

from __future__ import annotations

from typing import List, Dict, Optional
from datetime import datetime


def detect_inventory_shortage(
    cur,
    window_start: datetime,
    window_end: datetime,
    eligible_statuses: Optional[list] = None,
) -> List[Dict]:
    """
    Args:
        cur: psycopg2 cursor
        window_start, window_end: 탐지 시간 범위
        eligible_statuses: 특정 status만 대상으로 할 때 (예: ["PAID", "PICKING"])

    Returns:
        alerts: [
          {
            "anomaly_type": "INVENTORY_SHORTAGE",
            "anomaly_key": "...",
            "order_id": "...",
            "product_id": "...",
            "stock": 0,
            "message": "...",
          },
          ...
        ]
    """

    # ✅ 핵심 수정: orders/products에 public 스키마 명시
    sql = """
    SELECT
      o.order_id,
      o.product_id,
      o.current_status,
      o.updated_at,
      p.stock
    FROM public.orders o
    JOIN public.products p
      ON o.product_id = p.product_id
    WHERE o.updated_at >= %(window_start)s
      AND o.updated_at <  %(window_end)s
      AND p.stock <= 0
    """

    params = {
        "window_start": window_start,
        "window_end": window_end,
    }

    # (선택) 특정 status만 대상으로 제한
    if eligible_statuses:
        sql += " AND o.current_status = ANY(%(eligible_statuses)s)"
        params["eligible_statuses"] = eligible_statuses

        # 만약 환경에서 ANY(list) 캐스팅 에러 나면 아래로 바꿔:
        # sql += " AND o.current_status = ANY(%(eligible_statuses)s::text[])"

    sql += " ORDER BY o.updated_at DESC;"

    cur.execute(sql, params)
    rows = cur.fetchall()

    alerts: List[Dict] = []
    for order_id, product_id, current_status, updated_at, stock in rows:
        anomaly_key = f"inventory_shortage|{order_id}|{product_id}"
        alerts.append(
            {
                "anomaly_type": "INVENTORY_SHORTAGE",
                "anomaly_key": anomaly_key,
                "order_id": order_id,
                "product_id": product_id,
                "current_status": current_status,
                "updated_at": updated_at.isoformat() if updated_at else None,
                "stock": int(stock) if stock is not None else None,
                "message": f"[재고 부족] {order_id} 상품({product_id}) 재고={stock}",
            }
        )

    return alerts