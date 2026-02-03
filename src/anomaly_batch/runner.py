"""
runner.py
- 배치 이상탐지 '실행 버튼' (엔트리포인트)

이 파일이 하는 일:
1) Postgres 접속
2) 최근 window_minutes 동안 들어온 데이터 대상으로 탐지 수행
   - 재고 부족(inventory_shortage)
   - 중복 이벤트 폭탄(duplicate_event)
3) 탐지 결과를 events 테이블에 event_type='AI_INSIGHT' 로 저장
4) 종료

실행:
    python src/anomaly_batch/runner.py

※ "1분마다 자동"으로 하고 싶으면:
- runner.py가 자동으로 도는 게 아니라
- cron 같은 스케줄러가 이 파일을 1분마다 실행해줘야 함
"""

import os
import time
import uuid
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import Json

# 같은 폴더의 파일 import (경로 단순)
from inventory_shortage import detect_inventory_shortage
from duplicate_event import detect_duplicate_event_bomb


# =============================================================================
# 환경변수 (consumer와 맞춰두면 운영이 편함)
# =============================================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

# 배치 윈도우: 최근 몇 분을 검사할지 (테스트용은 1~2분 추천)
WINDOW_MINUTES = int(os.getenv("ANOMALY_WINDOW_MINUTES", "2"))

# 중복 이벤트 폭탄 기준 (테스트면 5~10이 보기 좋음)
DUPLICATE_THRESHOLD = int(os.getenv("DUPLICATE_THRESHOLD", "10"))

# 같은 anomaly_key로 AI_INSIGHT를 너무 자주 남기지 않기 위한 쿨다운(분)
# 예: 3분이면, 같은 이슈가 연속으로 탐지돼도 3분 내엔 1번만 기록
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "3"))

# 재고 부족 탐지에서 특정 status만 보고 싶다면 콤마로 지정 가능
# 예: "ORDER_CREATED,PAYMENT_COMPLETED"
INVENTORY_ELIGIBLE_STATUSES = os.getenv("INVENTORY_ELIGIBLE_STATUSES", "").strip()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# DB 연결 (재시도)
# =============================================================================
def connect_db_with_retry():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            print("✅ Postgres 연결 성공")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# =============================================================================
# AI_INSIGHT 저장 SQL
# - events 테이블에 탐지 결과를 이벤트로 남기면
#   대시보드/로그/추적 관점에서 '운영 이벤트'로 다루기 쉬움
# =============================================================================
SQL_INSERT_AI_INSIGHT = """
INSERT INTO events (
  event_id,
  order_id,
  event_type,
  reason_code,
  occurred_at,
  ingested_at,
  source,
  payload_json
) VALUES (
  %(event_id)s,
  %(order_id)s,
  %(event_type)s,
  %(reason_code)s,
  %(occurred_at)s,
  %(ingested_at)s,
  %(source)s,
  %(payload_json)s
)
ON CONFLICT (event_id) DO NOTHING;
"""


def already_alerted_recently(cur, anomaly_key: str, cooldown_minutes: int) -> bool:
    """
    같은 anomaly_key로 AI_INSIGHT가 최근에 이미 저장됐는지 확인.
    - 배치가 매 1분마다 돌면, 동일 이상을 매번 또 적재하는 '스팸'이 생길 수 있음
    - 그래서 cooldown 동안은 1번만 남기도록 막아줌
    """
    sql = """
    SELECT 1
    FROM events
    WHERE event_type = 'AI_INSIGHT'
      AND ingested_at >= (now() - (%(cooldown_minutes)s || ' minutes')::interval)
      AND payload_json->>'anomaly_key' = %(anomaly_key)s
    LIMIT 1;
    """
    cur.execute(
        sql,
        {"cooldown_minutes": cooldown_minutes, "anomaly_key": anomaly_key},
    )
    return cur.fetchone() is not None


def parse_status_list(value: str):
    """
    "A,B,C" 형태 env를 ["A","B","C"]로 변환
    """
    if not value:
        return None
    items = [x.strip() for x in value.split(",") if x.strip()]
    return items if items else None


def run_once():
    """
    배치 1회 실행:
    - window 계산
    - 탐지 2개 수행
    - AI_INSIGHT 이벤트 저장
    """
    window_end = now_utc()
    window_start = window_end - timedelta(minutes=WINDOW_MINUTES)

    print("=" * 70)
    print("🕵️ anomaly batch run")
    print(f" - window_start: {window_start.isoformat()}")
    print(f" - window_end  : {window_end.isoformat()}")
    print(f" - duplicate_threshold: {DUPLICATE_THRESHOLD}")
    print(f" - cooldown_minutes   : {ALERT_COOLDOWN_MINUTES}")
    print("=" * 70)

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        eligible_statuses = parse_status_list(INVENTORY_ELIGIBLE_STATUSES)

        # ------------------------------------------------------------
        # 1) 재고 부족 탐지
        # ------------------------------------------------------------
        inv_alerts = detect_inventory_shortage(
            cur=cur,
            window_start=window_start,
            window_end=window_end,
            eligible_statuses=eligible_statuses,
        )

        # ------------------------------------------------------------
        # 2) 중복 이벤트 폭탄 탐지
        # ------------------------------------------------------------
        dup_alerts = detect_duplicate_event_bomb(
            cur=cur,
            window_start=window_start,
            window_end=window_end,
            threshold=DUPLICATE_THRESHOLD,
        )

        all_alerts = inv_alerts + dup_alerts

        if not all_alerts:
            print("✅ 탐지된 이상 없음")
            conn.commit()
            return

        print(f"⚠️ 탐지된 이상 개수: {len(all_alerts)}")

        # ------------------------------------------------------------
        # 3) 탐지 결과를 events에 AI_INSIGHT로 저장
        # ------------------------------------------------------------
        saved = 0
        skipped = 0

        for alert in all_alerts:
            anomaly_key = alert["anomaly_key"]

            # 같은 이슈를 너무 자주 쌓지 않게 쿨다운 적용
            if already_alerted_recently(cur, anomaly_key, ALERT_COOLDOWN_MINUTES):
                skipped += 1
                continue

            # events에 넣을 payload_json
            # - 탐지 타입/키/메시지/윈도우/세부정보를 모두 넣어두면
            #   나중에 대시보드에서 바로 보여주기 쉬움
            payload = {
                "anomaly_type": alert.get("anomaly_type"),
                "anomaly_key": anomaly_key,
                "message": alert.get("message"),
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "details": alert,  # 세부정보 통째로 넣기(디버깅 편함)
                "detected_at": now_utc().isoformat(),
            }

            # order_id가 있는 탐지면 order_id를 넣고,
            # (없으면 NULL로 들어감. 스키마가 NOT NULL이면 여기서 막아야 함)
            order_id = alert.get("order_id")

            cur.execute(
                SQL_INSERT_AI_INSIGHT,
                {
                    "event_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "event_type": "AI_INSIGHT",
                    # reason_code를 anomaly_type으로 넣어두면 필터링이 편해짐
                    "reason_code": alert.get("anomaly_type"),
                    "occurred_at": now_utc(),
                    "ingested_at": now_utc(),
                    "source": "anomaly-batch",
                    "payload_json": Json(payload),
                },
            )
            saved += 1

        conn.commit()
        print(f"✅ AI_INSIGHT 저장 완료: {saved}건 (쿨다운으로 스킵: {skipped}건)")

    except Exception as e:
        conn.rollback()
        print(f"❌ 배치 실행 실패: {e}")
        raise
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    # 배치 1회 실행 후 종료
    run_once()
