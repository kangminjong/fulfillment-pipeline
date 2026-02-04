import asyncio
import os
from datetime import datetime, timezone

import asyncpg
from pathlib import Path

_BASE = Path(__file__).resolve().parent / "sql"
_CACHE = {}

def load_sql(filename: str) -> str:
    if filename in _CACHE:
        return _CACHE[filename]
    path = _BASE / filename
    sql = path.read_text(encoding="utf-8")
    _CACHE[filename] = sql
    return sql


def seconds_until_next_minute() -> int:
    now = datetime.now(timezone.utc)
    return 60 - now.second


async def run():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL 환경변수가 필요합니다(.env 확인).")

    sql = load_sql("rollup_metrics_window.sql")

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    try:
        # 시작하자마자 1회 실행(바로 row 생기게)
        async with pool.acquire() as conn:
            await conn.execute(sql)

        while True:
            await asyncio.sleep(seconds_until_next_minute())
            async with pool.acquire() as conn:
                await conn.execute(sql)
            print("[metrics-worker] rollup ok")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
