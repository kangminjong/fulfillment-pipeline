"""
runner.py
- anomaly_batch 실행 엔트리포인트
- ✅ 10초마다 계속 반복 실행 (개발용)
- ✅ 이상 탐지 결과를 "ANOMALY_ALERT"로 모아서 콘솔에 출력 + DB(public.events)에 저장

동작 요약(매 10초마다):
1) 최근 WINDOW_MINUTES 동안 데이터를 조회해서 이상 탐지
   - 재고 부족(inventory_shortage)
   - 중복 이벤트 폭탄(duplicate_event_bomb)
2) 이번 회차 탐지 결과를 "ANOMALY_ALERT" 1건(요약)으로 저장
   - public.events.event_type = 'ANOMALY_ALERT'
   - payload_json에 summary + details 저장
3) 콘솔에도 요약 출력
4) 10초 sleep 후 반복

실행:
  python3 -m src.anomaly_batch.runner

중요:
- 이 파일은 개발/데모용 무한루프.
- 운영에서는 cron/Airflow/K8s CronJob 권장.
"""

import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import Json

# =============================================================================
# anomaly_batch 내부 모듈 (상대 import)
# =============================================================================
from .inventory_shortage import detect_inventory_shortage
from .duplicate import detect_duplicate_event_bomb

# =============================================================================
# Postgres 접속 정보
# ✅ localhost 사용 안 함 (요구사항)
# =============================================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.239.40")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fulfillment")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

# =============================================================================
# 배치 설정
# =============================================================================
# 최근 몇 분(window)을 검사할지
WINDOW_MINUTES = int(os.getenv("ANOMALY_WINDOW_MINUTES", "2"))

# 중복 이벤트 폭탄 기준
DUPLICATE_THRESHOLD = int(os.getenv("DUPLICATE_THRESHOLD", "10"))

# 같은 이슈(anomaly_key)를 너무 자주 쌓지 않기 위한 쿨다운(분)
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "3"))

# ✅ 개발용: 10초마다 반복 실행
INTERVAL_SECONDS = int(os.getenv("ANOMALY_INTERVAL_SECONDS", "10"))

# (선택) 재고 부족 탐지에서 특정 status만 보고 싶다면 콤마로 지정
# 예: "PAID,PICKING"
INVENTORY_ELIGIBLE_STATUSES = os.getenv("INVENTORY_ELIGIBLE_STATUSES", "").strip()

# ✅ 결과 이벤트 타입 이름 (요구사항)
ALERT_EVENT_TYPE = "ANOMALY_ALERT"

# ✅ 콘솔에 상세 목록까지 찍을지 (너무 길면 0으로)
PRINT_DETAILS = os.getenv("ANOMALY_PRINT_DETAILS", "1").strip()  # "1" / "0"


# =============================================================================
def now_utc() -> datetime:
    """UTC now (timezone-aware)"""
    return datetime.now(timezone.utc)


def parse_status_list(value: str) -> Optional[List[str]]:
    """"A,B,C" 형태 env를 ["A","B","C"]로 변환"""
    if not value:
        return None
    items = [x.strip() for x in value.split(",") if x.strip()]
    return items if items else None


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
            print(f"✅ Postgres 연결 성공 ({POSTGRES_HOST})")
            return conn
        except Exception as e:
            print(f"⏳ Postgres 연결 실패: {e} (3초 후 재시도)")
            time.sleep(3)


# =============================================================================
# public.events에 ANOMALY_ALERT 저장 (스키마 명시)
# =============================================================================
SQL_INSERT_ALERT = """
INSERT INTO public.events (
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
    같은 anomaly_key로 최근에 이미 ANOMALY_ALERT가 저장됐는지 확인.
    - 배치가 자주 돌면 동일 이상을 계속 저장하는 스팸을 막기 위해 사용.
    """
    sql = """
    SELECT 1
    FROM public.events
    WHERE event_type = %(event_type)s
      AND ingested_at >= (now() - (%(cooldown_minutes)s || ' minutes')::interval)
      AND payload_json->>'anomaly_key' = %(anomaly_key)s
    LIMIT 1;
    """
    cur.execute(
        sql,
        {
            "event_type": ALERT_EVENT_TYPE,
            "cooldown_minutes": cooldown_minutes,
            "anomaly_key": anomaly_key,
        },
    )
    return cur.fetchone() is not None


def summarize_alerts(alerts: List[Dict]) -> Dict:
    """
    이번 회차 alerts를 요약해서
    - total
    - by_type
    - top_keys(상위 anomaly_key 몇 개)
    를 만든다.
    """
    by_type: Dict[str, int] = {}
    for a in alerts:
        t = a.get("anomaly_type") or "UNKNOWN"
        by_type[t] = by_type.get(t, 0) + 1

    # anomaly_key 상위 몇 개만 뽑기 (동일 키가 여러 번 들어올 수 있음)
    key_cnt: Dict[str, int] = {}
    for a in alerts:
        k = a.get("anomaly_key") or "NO_KEY"
        key_cnt[k] = key_cnt.get(k, 0) + 1

    top_keys = sorted(key_cnt.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "total": len(alerts),
        "by_type": by_type,
        "top_keys": top_keys,
    }


def print_console_summary(window_start: datetime, window_end: datetime, alerts: List[Dict]):
    """
    콘솔에 이번 회차 요약을 찍는다.
    """
    if not alerts:
        print("✅ 탐지된 이상 없음")
        return

    summary = summarize_alerts(alerts)
    print(f"🚨 [{ALERT_EVENT_TYPE}] 이번 회차 탐지 {summary['total']}건")
    for t, cnt in summary["by_type"].items():
        print(f"  - {t}: {cnt}건")

    if PRINT_DETAILS == "1":
        # 너무 길어질 수 있어서 간단히만
        for a in alerts[:20]:
            msg = a.get("message") or ""
            print(f"    • {a.get('anomaly_type')} | {a.get('anomaly_key')} | {msg}")
        if len(alerts) > 20:
            print(f"    ... (상세 {len(alerts)}건 중 20건만 표시)")


# =============================================================================
# 배치 1회 실행
# =============================================================================
def run_once():
    window_end = now_utc()
    window_start = window_end - timedelta(minutes=WINDOW_MINUTES)

    print("=" * 70)
    print("🕵️ anomaly batch run")
    print(f" - window_start: {window_start.isoformat()}")
    print(f" - window_end  : {window_end.isoformat()}")
    print(f" - interval    : {INTERVAL_SECONDS}s")
    print(f" - window_min  : {WINDOW_MINUTES}m")
    print(f" - dup_thresh  : {DUPLICATE_THRESHOLD}")
    print("=" * 70)

    conn = connect_db_with_retry()
    cur = conn.cursor()

    try:
        eligible_statuses = parse_status_list(INVENTORY_ELIGIBLE_STATUSES)

        # ------------------------------------------------------------
        # 1) 재고 부족 탐지 (DB 쿼리 기반)
        # ------------------------------------------------------------
        inv_alerts = detect_inventory_shortage(
            cur=cur,
            window_start=window_start,
            window_end=window_end,
            eligible_statuses=eligible_statuses,
        )

        # ------------------------------------------------------------
        # 2) 중복 이벤트 폭탄 탐지 (DB 쿼리 기반)
        # ------------------------------------------------------------
        dup_alerts = detect_duplicate_event_bomb(
            cur=cur,
            window_start=window_start,
            window_end=window_end,
            threshold=DUPLICATE_THRESHOLD,
        )

        # 이번 회차 탐지 결과
        all_alerts: List[Dict] = inv_alerts + dup_alerts

        # ✅ 콘솔에는 “이번 회차 결과”를 무조건 출력
        print_console_summary(window_start, window_end, all_alerts)

        if not all_alerts:
            # 이상 없으면 저장할 것도 없으니 종료
            conn.commit()
            return

        # ------------------------------------------------------------
        # 3) DB에 ANOMALY_ALERT 저장
        #    - "이번 회차 요약 1건" + (선택) 개별 alert도 저장할 수 있음
        #
        # 여기서는: "요약 1건"만 저장 (너가 원한 '모아서 알려주기'에 가장 맞음)
        # ------------------------------------------------------------
        # 쿨다운은 "개별 anomaly_key" 기준으로 걸 수도 있지만,
        # 지금은 "요약 1건"만 저장할 거라서 쿨다운을 "요약 키"에 걸어줌.
        summary_key = f"summary|{window_start.isoformat()}|{window_end.isoformat()}"
        if already_alerted_recently(cur, summary_key, ALERT_COOLDOWN_MINUTES):
            # (거의 발생 안 하겠지만) 동일 window로 다시 도는 상황 방지
            conn.commit()
            return

        summary = summarize_alerts(all_alerts)

        payload = {
            "anomaly_type": "SUMMARY",
            "anomaly_key": summary_key,
            "event_type": ALERT_EVENT_TYPE,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "total": summary["total"],
            "by_type": summary["by_type"],
            "top_keys": summary["top_keys"],
            # 필요하면 상세도 같이 넣어둔다 (디버깅/대시보드에서 원클릭 확인 가능)
            "details": all_alerts,
            "detected_at": now_utc().isoformat(),
        }

        cur.execute(
            SQL_INSERT_ALERT,
            {
                "event_id": str(uuid.uuid4()),
                # 요약 알림은 특정 주문 1개에 귀속되지 않을 수 있음 → order_id는 None
                "order_id": None,
                "event_type": ALERT_EVENT_TYPE,
                "reason_code": "SUMMARY",
                "occurred_at": now_utc(),
                "ingested_at": now_utc(),
                "source": "anomaly-batch",
                "payload_json": Json(payload),
            },
        )

        conn.commit()
        print(f"✅ [{ALERT_EVENT_TYPE}] 요약 1건 저장 완료 (total={summary['total']})")

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


# =============================================================================
# 메인 루프 (10초마다 계속 실행)
# =============================================================================
if __name__ == "__main__":
    print(f"🚀 anomaly_batch runner 시작 ({INTERVAL_SECONDS}초 주기) | event_type={ALERT_EVENT_TYPE}")
    while True:
        run_once()
        time.sleep(INTERVAL_SECONDS)