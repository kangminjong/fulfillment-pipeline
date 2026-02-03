"""
duplicate.py
- 최근 N초/분(window) 동안 특정 order_id + event_type 조합이
  비정상적으로 많이 쌓이면 "중복 이벤트 폭탄"으로 탐지한다.

DB 기준:
- public.events 테이블의 ingested_at(수집시각)을 기준으로 window를 잡는다.
- group by: order_id, event_type

⚠️ 중요:
- window(예: 10초)는 이 함수 안에서 정해지는 게 아니라,
  호출하는 쪽에서 window_start/window_end를 어떻게 만들었는지에 따라 결정된다.
"""

from __future__ import annotations

from typing import List, Dict
from datetime import datetime


def detect_duplicate_event_bomb(
    cur,
    window_start: datetime,
    window_end: datetime,
    threshold: int = 10,
) -> List[Dict]:
    """
    최근 window 구간 내에서 (order_id, event_type) 조합이 threshold 이상 발생하면 탐지한다.

    Args:
        cur:
            psycopg2 cursor
            - 이미 DB connection에서 cur = conn.cursor() 로 만든 커서라고 가정
        window_start, window_end:
            탐지 시간 범위
            - 이 구간은 ingested_at 기준으로 필터링한다.
            - "10초 테스트"를 하려면 호출부에서
              window_start = now - 10초, window_end = now 로 넣어야 한다.
            - UTC 권장(서버/컨테이너 간 시간 혼선 방지)
        threshold:
            이 횟수 이상이면 "중복 이벤트 폭탄"으로 판단
            - 기본값 10

    Returns:
        alerts: List[Dict]
            예시:
            [
              {
                "anomaly_type": "DUPLICATE_EVENT_BOMB",
                "anomaly_key": "duplicate_event_bomb|ORDER123|ORDER_CREATED",
                "order_id": "ORDER123",
                "event_type": "ORDER_CREATED",
                "count": 123,
                "message": "[중복 이벤트 폭탄] ORDER123에서 ORDER_CREATED 이벤트가 123회 발생",
              },
              ...
            ]
    """

    # -------------------------------------------------------------------------
    # 1) SQL: window 구간 내에서 order_id + event_type 단위로 카운트
    # -------------------------------------------------------------------------
    # ✅ 스키마 명시(public.events):
    # - 검색 경로(search_path) 문제로 테이블을 못 찾는 케이스를 방지
    #
    # ✅ 시간 조건:
    # - [window_start, window_end) 형태 (start 포함, end 미포함)
    #   -> 윈도우를 연속으로 돌릴 때 중복 집계를 방지하기 좋음
    #
    # ✅ NULL 방지:
    # - order_id나 event_type이 NULL이면 grouping 의미가 없고,
    #   anomaly_key 만들기도 애매하므로 제외
    sql = """
    SELECT
      e.order_id,
      e.event_type,
      COUNT(*) AS cnt
    FROM public.events e
    WHERE e.ingested_at >= %(window_start)s
      AND e.ingested_at <  %(window_end)s
      AND e.order_id IS NOT NULL
      AND e.event_type IS NOT NULL
    GROUP BY e.order_id, e.event_type
    HAVING COUNT(*) >= %(threshold)s
    ORDER BY cnt DESC;
    """

    # -------------------------------------------------------------------------
    # 2) 파라미터 바인딩
    # -------------------------------------------------------------------------
    # - psycopg2의 named parameter 스타일(%(name)s)에 맞춰 dict로 전달
    params = {
        "window_start": window_start,
        "window_end": window_end,
        "threshold": threshold,
    }

    # -------------------------------------------------------------------------
    # 3) 쿼리 실행 및 결과 수집
    # -------------------------------------------------------------------------
    cur.execute(sql, params)
    rows = cur.fetchall()
    # rows는 보통 이런 형태:
    # [
    #   (order_id1, event_type1, cnt1),
    #   (order_id2, event_type2, cnt2),
    #   ...
    # ]

    # -------------------------------------------------------------------------
    # 4) 결과를 alerts 포맷으로 변환
    # -------------------------------------------------------------------------
    alerts: List[Dict] = []

    for order_id, event_type, cnt in rows:
        # anomaly_key:
        # - 같은 폭탄을 "동일한 것"으로 묶기 위한 고유 키
        # - 나중에 upsert/중복알림 방지/알람 집계에 쓰기 좋음
        anomaly_key = f"duplicate_event_bomb|{order_id}|{event_type}"

        alerts.append(
            {
                "anomaly_type": "DUPLICATE_EVENT_BOMB",
                "anomaly_key": anomaly_key,
                "order_id": order_id,
                "event_type": event_type,
                "count": int(cnt),
                "message": f"[중복 이벤트 폭탄] {order_id}에서 {event_type} 이벤트가 {cnt}회 발생",
            }
        )

    return alerts