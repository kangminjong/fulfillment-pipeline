from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, InterfaceError, DatabaseError

 
@dataclass
class PostgresRepository:
    """
    ✅ DB 접근 전담 레이어 (consumer/handler에서만 호출)

    역할:
    - events 테이블 insert (중복 event_id는 무시)
    - order_current 테이블 upsert (주문별 최신상태 유지, out-of-order 방지)

    handler.py에서 기대하는 메서드:
    - insert_event_only(e)
    - insert_event_and_return_inserted(e) -> bool
    - upsert_order_current(...)
    - close()
    """
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int = 5

    def __post_init__(self) -> None:
        self._conn = self._connect()

    # -------------------------------------------------------------------------
    # Connection 관리
    # -------------------------------------------------------------------------
    def _connect(self):
        """
        ✅ psycopg2 커넥션 생성
        - autocommit=False (트랜잭션으로 안전하게)
        """
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
        )
        conn.autocommit = False
        return conn

    def _ensure_conn(self) -> None:
        """
        ✅ 커넥션이 끊긴 경우 재연결 시도
        - DB가 잠깐 죽었다가 살아나는 운영 상황 대비
        """
        try:
            if self._conn is None or self._conn.closed != 0:
                self._conn = self._connect()
        except Exception:
            # 재연결 실패는 호출부에서 예외로 처리하게 둔다
            raise

    def close(self) -> None:
        """리소스 정리."""
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass

    def _cursor(self):
        """
        cursor를 매번 새로 생성 (트랜잭션은 with self._conn: 로 제어)
        """
        self._ensure_conn()
        return self._conn.cursor()

    # -------------------------------------------------------------------------
    # events 테이블 적재
    # -------------------------------------------------------------------------
    def insert_event_only(self, e: Dict[str, Any]) -> None:
        """
        ✅ events 테이블에 기록만 남김 (PARSE_ERROR/SCHEMA_MISSING 같은 이벤트)
        - 중복(event_id 충돌)은 DO NOTHING
        - 실패 시 예외를 올려서 main이 commit하지 않도록(재처리) 유도
        """
        self._ensure_conn()

        try:
            with self._conn:
                with self._cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO events (
                            event_id,
                            order_id,
                            event_type,
                            reason_code,
                            occurred_at,
                            source,
                            payload_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (
                            e["event_id"],
                            e.get("order_id"),
                            e["event_type"],
                            e.get("reason_code"),
                            e["occurred_at"],
                            e["source"],
                            psycopg2.extras.Json(e.get("payload_json", {})),
                        ),
                    )
        except (OperationalError, InterfaceError):
            # 커넥션이 죽었을 가능성 -> 다음 호출에서 재연결 시도
            self.close()
            raise

    def insert_event_and_return_inserted(self, e: Dict[str, Any]) -> bool:
        """
        ✅ 정상 이벤트: events에 먼저 insert하고 "신규 삽입인지"를 bool로 반환

        반환:
        - True  => 신규 삽입 성공 (order_current 갱신 진행해도 됨)
        - False => 이미 같은 event_id가 존재(중복) (order_current 갱신 스킵 가능)

        구현 포인트:
        - ON CONFLICT DO NOTHING + RETURNING으로 신규 여부 판정
        """
        self._ensure_conn()

        try:
            with self._conn:
                with self._cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO events (
                            event_id,
                            order_id,
                            event_type,
                            reason_code,
                            occurred_at,
                            source,
                            payload_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (event_id) DO NOTHING
                        RETURNING event_id
                        """,
                        (
                            e["event_id"],
                            e.get("order_id"),
                            e["event_type"],
                            e.get("reason_code"),
                            e["occurred_at"],
                            e["source"],
                            psycopg2.extras.Json(e.get("payload_json", {})),
                        ),
                    )
                    row = cur.fetchone()
                    return row is not None
        except (OperationalError, InterfaceError):
            self.close()
            raise

    # -------------------------------------------------------------------------
    # order_current 테이블 upsert (최신상태 유지)
    # -------------------------------------------------------------------------
    def upsert_order_current(
        self,
        order_id: str,
        current_stage: str,
        current_status: str,
        hold_reason_code: Optional[str],
        last_event_type: str,
        last_occurred_at: datetime,
        tracking_no: Optional[str],
        promised_delivery_date: Optional[str],
    ) -> None:
        """
        ✅ 주문별 최신 상태 1줄 유지 (order_current)

        핵심:
        - ON CONFLICT (order_id) DO UPDATE 로 upsert
        - out-of-order 방지:
          WHERE order_current.last_occurred_at <= EXCLUDED.last_occurred_at
          => 더 최신 이벤트만 현재 상태를 덮어씀

        * tracking_no / promised_delivery_date는 없는 경우가 많아서
          COALESCE로 기존 값 유지(새 값이 None이면 기존 값 유지)
        """
        self._ensure_conn()

        try:
            with self._conn:
                with self._cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO order_current (
                            order_id,
                            current_stage,
                            current_status,
                            hold_reason_code,
                            last_event_type,
                            last_occurred_at,
                            tracking_no,
                            promised_delivery_date
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::date)
                        ON CONFLICT (order_id) DO UPDATE
                        SET
                            current_stage = EXCLUDED.current_stage,
                            current_status = EXCLUDED.current_status,
                            hold_reason_code = EXCLUDED.hold_reason_code,
                            last_event_type = EXCLUDED.last_event_type,
                            last_occurred_at = EXCLUDED.last_occurred_at,
                            tracking_no = COALESCE(EXCLUDED.tracking_no, order_current.tracking_no),
                            promised_delivery_date = COALESCE(EXCLUDED.promised_delivery_date, order_current.promised_delivery_date),
                            updated_at = now()
                        WHERE
                            order_current.last_occurred_at IS NULL
                            OR order_current.last_occurred_at <= EXCLUDED.last_occurred_at
                        """,
                        (
                            order_id,
                            current_stage,
                            current_status,
                            hold_reason_code,
                            last_event_type,
                            last_occurred_at,
                            tracking_no,
                            promised_delivery_date,
                        ),
                    )
        except (OperationalError, InterfaceError):
            self.close()
            raise