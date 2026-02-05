import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import asyncpg

# ============================================================
# 0) 설정 (여기만 바꾸면 전체 동작을 컨트롤 가능)
# ============================================================

KST = ZoneInfo("Asia/Seoul")

# ✅ 버킷 크기(= metrics_window에 쌓이는 단위)
BUCKET = timedelta(minutes=30)  # "30분 단위로 쌓이게" 하는 핵심 값

# ✅ (A) 누락 채우기(백필) 켤지 말지
# - 테스트에서 과거를 안 돌리고 싶으면 0으로 꺼도 됨
ENABLE_CATCHUP = "1"

# ✅ (B) 백필 시작점을 어떻게 잡을지 2가지 옵션
# 1) BACKFILL_FROM(절대 시각) 지정하면 이게 최우선
#    예) BACKFILL_FROM="2026-02-01 00:00"
BACKFILL_FROM = "2026-01-20 00:00"  # str | None

# 2) BACKFILL_FROM이 없으면 최근 N일치만 채움
BACKFILL_DAYS = 30
# BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "30"))

# ✅ 최근 재집계 범위(늦게 들어오는 데이터 보정용)
# - 운영: 120 (2시간)
# - 테스트: 30 or 60 정도가 체감 좋음(버킷이 30분이니까)
REROLL_MINUTES = 120

_BASE = Path(__file__).resolve().parent / "sql"
_CACHE: dict[str, str] = {}


# ============================================================
# 1) 유틸
# ============================================================

def load_sql(filename: str) -> str:
    if filename in _CACHE:
        return _CACHE[filename]
    path = _BASE / filename
    sql = path.read_text(encoding="utf-8")
    _CACHE[filename] = sql
    return sql


def floor_to_bucket(dt: datetime) -> datetime:
    """
    dt를 '버킷 경계'로 내림.
    - 지금은 BUCKET=30분이므로 00분 또는 30분으로 내림됨.
    """
    bucket_minutes = int(BUCKET.total_seconds() // 60)
    minute = (dt.minute // bucket_minutes) * bucket_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def seconds_until_next_bucket(dt: datetime) -> int:
    """
    다음 버킷 경계(30분)까지 남은 초
    - 워커가 '30분마다' 실행되게 만드는 핵심
    """
    cur = floor_to_bucket(dt)
    nxt = cur + BUCKET
    return max(1, int((nxt - dt).total_seconds()))


async def init_conn(conn: asyncpg.Connection):
    # CURRENT_DATE, now() 같은 기준을 KST로 통일
    await conn.execute("SET TIME ZONE 'Asia/Seoul'")


async def get_max_window_end(conn: asyncpg.Connection):
    """
    metrics_window에 마지막으로 저장된 window_end를 가져옴.
    - 이 값이 오래된 날짜(예: 1월 6일)면
      catchup에서 그 다음부터 계속 메우려고 함.
    """
    row = await conn.fetchrow("SELECT MAX(window_end) AS max_end FROM metrics_window")
    return row["max_end"]


def parse_backfill_from(s: str) -> datetime:
    """
    BACKFILL_FROM="YYYY-MM-DD HH:MM" 형태를 KST tz-aware datetime으로 변환
    """
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=KST)


# ============================================================
# 2) 집계 실행
# ============================================================

async def rollup_range(conn: asyncpg.Connection, sql: str, start: datetime, end: datetime) -> int:
    """
    [start, end) 범위를 BUCKET(30분) 단위로 집계해서 저장한다.
    - sql은 (window_start, window_end) 파라미터를 받아야 함.
    - 이미 같은 버킷 row가 있으면 ON CONFLICT로 UPDATE(덮어쓰기)
    """
    if start >= end:
        return 0
    print("???????")
    filled = 0
    ws = start
    while ws < end:
        we = ws + BUCKET
        if we > end:
            break
        print("////",ws,"++++",we)
        await conn.execute(sql, ws, we)
        filled += 1
        ws = we
    return filled


async def rollup_missing_then_reroll_recent(conn: asyncpg.Connection, sql: str) -> tuple[int, int]:
    """
    1) (선택) 과거 누락 구간 채우기(catchup/backfill)
    2) (항상) 최근 REROLL_MINUTES 구간 재집계(늦게 들어온 데이터 보정)
    """
    now = datetime.now(KST)
    print("테스트: ", now)
    # ✅ "완료된 마지막 버킷의 끝" (예: 현재 10:17이면 target_end=10:00)
    target_end = floor_to_bucket(now)

    max_end = await get_max_window_end(conn)

    # ------------------------------------------------------------
    # (1) catchup/backfill: 여기 때문에 1월 6일부터 돌아갈 수 있음
    # ------------------------------------------------------------
    catchup = 0
    if ENABLE_CATCHUP:
        # ✅ start 결정 로직 = 네가 컨트롤하고 싶은 부분

        if BACKFILL_FROM:
            # (옵션1) 절대 시작 시각을 직접 지정
            # 예: BACKFILL_FROM="2026-02-01 00:00"
            start = floor_to_bucket(parse_backfill_from(BACKFILL_FROM))

        elif max_end is None:
            # (옵션2) metrics_window가 비어있으면 최근 BACKFILL_DAYS만 채움
            # 오늘(2/4) 기준 30일이면 1/5~1/6부터 시작 -> 너가 본 "1월 6일"이 여기서 나옴
            start = floor_to_bucket(target_end - timedelta(days=BACKFILL_DAYS))

        else:
            # (옵션3) metrics_window가 이미 있으면, 마지막 window_end 다음부터 이어서 채움
            # 만약 max_end가 1월 6일이면 -> 1월 6일 이후를 전부 채우려고 시작함
            start = floor_to_bucket(max_end)

        # ✅ start ~ target_end 구간을 30분 버킷으로 모두 채움
        catchup = await rollup_range(conn, sql, start, target_end)

    # ------------------------------------------------------------
    # (2) reroll: 최근 N분은 항상 다시 계산해서 UPDATE(덮어쓰기)
    # ------------------------------------------------------------
    reroll_from = target_end - timedelta(minutes=REROLL_MINUTES)
    reroll_start = floor_to_bucket(reroll_from)

    reroll = await rollup_range(conn, sql, reroll_start, target_end)

    return catchup, reroll


# ============================================================
# 3) main loop
# ============================================================

async def run():
    dsn = "postgresql://admin:admin@192.168.239.40:5432/fulfillment"
    if not dsn:
        raise RuntimeError("DATABASE_URL 환경변수가 필요합니다(.env 확인).")

    sql = load_sql("rollup_metrics_window.sql")

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=3,
        init=init_conn,
    )
    print("이건 되냐?")
    try:
        print("하하하")
        while True:
            print("여기느?")
            async with pool.acquire() as conn:
                catchup, reroll = await rollup_missing_then_reroll_recent(conn, sql)
                print(
                    f"[metrics-worker] "
                    f"catchup={catchup} (enabled={ENABLE_CATCHUP}, from={BACKFILL_FROM}, days={BACKFILL_DAYS}), "
                    f"reroll(last {REROLL_MINUTES}m)={reroll}"
                )

            # ✅ 다음 30분 경계까지 대기 -> 30분 단위 실행 유지
            await asyncio.sleep(seconds_until_next_bucket(datetime.now(KST)))

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
