"""
duplicate_event.py
- "동일 주문의 중복 이벤트 폭탄" 탐지 로직만 담당

아이디어(배치용):
- 최근 N분 동안 events 테이블에서
- order_id + event_type 조합이
- threshold 이상 발생하면 폭탄으로 판단

※ 왜 ingested_at을 쓰냐?
- occurred_at은 producer가 이상하게 보내거나 누락될 수 있음
- ingested_at은 "DB에 들어온 시각"이라 배치 윈도우 기준 잡기 안정적
"""

from typing import Dict, List


def detect_duplicate_event_bomb(cur, window_start, window_end, threshold: int = 10) -> List[Dict]:
    """
    동일 주문 중복 이벤트 폭탄 탐지

    Args:
        cur: psycopg2 cursor
        window_start, window_end: 탐지 시간 범위(timestamptz)
        threshold: 이 횟수 이상이면 "폭탄"으로 판단

    Returns:
        alerts: 이상탐지 결과 리스트(dict)
    """

    # AI_INSIGHT(우리 탐지 결과)는 집계에서 제외해야 무한루프 느낌이 안 남
    sql = """
    SELECT
        order_id,
        event_type,
        COUNT(*) AS cnt,
        MIN(ingested_at) AS first_ingested_at,
        MAX(ingested_at) AS last_ingested_at
    FROM events
    WHERE ingested_at >= %(window_start)s
      AND ingested_at <  %(window_end)s
      AND order_id IS NOT NULL
      AND event_type IS NOT NULL
      AND event_type <> 'AI_INSIGHT'
    GROUP BY order_id, event_type
    HAVING COUNT(*) >= %(threshold)s
    ORDER BY cnt DESC;
    """

    cur.execute(
        sql,
        {
            "window_start": window_start,
            "window_end": window_end,
            "threshold": threshold,
        },
    )

    rows = cur.fetchall()

    alerts = []
    for (order_id, event_type, cnt, first_ingested_at, last_ingested_at) in rows:
        anomaly_key = f"duplicate_event_bomb|order:{order_id}|type:{event_type}"

        alerts.append(
            {
                "anomaly_type": "DUPLICATE_EVENT_BOMB",
                "anomaly_key": anomaly_key,
                "order_id": order_id,
                "event_type": event_type,
                "count": int(cnt),
                "first_ingested_at": first_ingested_at.isoformat() if first_ingested_at else None,
                "last_ingested_at": last_ingested_at.isoformat() if last_ingested_at else None,
                "message": f"최근 윈도우에서 동일 이벤트가 {cnt}번 발생했습니다.",
            }
        )

    return alerts