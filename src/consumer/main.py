import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from .handler import handle_message
from .repository import PostgresRepository


def env(name: str, default: str) -> str:
    """환경변수 없으면 default 사용."""
    v = os.getenv(name)
    return v if v is not None and v != "" else default


@dataclass
class RawFileSink:
    """
    ✅ '원본 JSON'을 파일로 저장하는 역할

    - 파일 경로에 topic/partition/offset을 포함해 1메시지=1파일로 저장
      => 같은 메시지를 재처리해도 같은 경로라 중복 파일 생성 방지(idempotent)
    - tmp 파일에 먼저 쓰고 os.replace로 교체
      => 쓰다가 죽어도 깨진 파일이 남을 가능성 낮음(atomic write)
    """
    base_dir: Path

    def _path_for(self, topic: str, partition: int, offset: int) -> Path:
        return self.base_dir / topic / f"partition_{partition}" / f"offset_{offset}.json"

    def save_if_absent(self, topic: str, partition: int, offset: int, raw_text: str) -> Path:
        path = self._path_for(topic, partition, offset)
        path.parent.mkdir(parents=True, exist_ok=True)

        # 이미 저장된 메시지면 그대로 반환 (재처리 시 중복 저장 방지)
        if path.exists():
            return path

        # atomic write: tmp -> replace
        tmp_path = path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(raw_text)

        os.replace(tmp_path, path)
        return path


def commit_one(consumer: KafkaConsumer, topic: str, partition: int, offset: int) -> None:
    """
    ✅ 수동 커밋(딱 한 메시지)

    Kafka의 commit은 '다음에 읽을 offset'을 저장하는 개념이라 offset+1을 커밋해야 함.
    예: offset=10 처리 완료 -> commit(11)
    """
    tp = TopicPartition(topic, partition)
    consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})


def main() -> int:
    # -----------------------------
    # 설정(토픽/브로커/그룹) + 저장 위치
    # -----------------------------
    topic = env("KAFKA_TOPIC", "event")  # ✅ 너희 토픽 이름
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = env("KAFKA_GROUP_ID", "fulfillment-consumer-v1")

    # raw 파일 저장 위치 (도커면 보통 볼륨 마운트 권장)
    raw_dir = Path(env("RAW_SAVE_DIR", "./data/raw")).resolve()
    raw_sink = RawFileSink(base_dir=raw_dir)

    # -----------------------------
    # DB 연결 (events/order_current 적재용)
    # -----------------------------
    repo = PostgresRepository(
        host=env("POSTGRES_HOST", "192.168.239.40"),
        port=int(env("POSTGRES_PORT", "5432")),
        dbname=env("POSTGRES_DB", "fulfillment"),
        user=env("POSTGRES_USER", "admin"),
        password=env("POSTGRES_PASSWORD", "admin"),
    )

    # -----------------------------
    # Kafka Consumer 생성
    # -----------------------------
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,

        # ✅ auto commit 끔:
        # 파일 저장 + DB 저장이 "둘 다 성공"했을 때만 commit 하려는 전략
        enable_auto_commit=False,

        auto_offset_reset=env("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        value_deserializer=lambda v: v,  # bytes 그대로 받기
        key_deserializer=lambda v: v,
    )

    # -----------------------------
    # Graceful shutdown: SIGTERM/SIGINT 받으면 루프 탈출
    # poll(timeout_ms=1000) 덕분에 최대 1초 내 종료 가능
    # -----------------------------
    running = True

    def _stop(_sig, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # poll 옵션
    poll_timeout_ms = int(env("KAFKA_POLL_TIMEOUT_MS", "1000"))
    max_records = int(env("KAFKA_MAX_RECORDS", "50"))

    # 처리 재시도 횟수(DB 일시 장애 대비)
    max_retries = int(env("CONSUME_MAX_RETRIES", "3"))

    print(f"[consumer] start | topic={topic} | bootstrap={bootstrap} | group={group_id}", flush=True)
    print(f"[consumer] raw_save_dir={raw_dir}", flush=True)
    print(f"[consumer] poll_timeout_ms={poll_timeout_ms} | max_records={max_records}", flush=True)

    try:
        while running:
            # -----------------------------
            # ✅ next() 대신 poll():
            # - 메시지 없으면 timeout 후 돌아옴
            # - running 플래그 확인 가능 => 종료 빨라짐
            # -----------------------------
            try:
                polled = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)
            except Exception as e:
                print(f"[consumer] kafka poll error: {e}", file=sys.stderr, flush=True)
                time.sleep(1)
                continue

            if not polled:
                continue

            # polled: {TopicPartition: [ConsumerRecord, ...], ...}
            # -----------------------------
            # ✅ 핵심 안정성 정책:
            # 같은 partition에서 앞 offset이 실패했는데
            # 뒤 offset을 커밋하면 "앞 메시지 유실" 가능
            # => partition 단위로 순서대로 처리하고,
            #    실패하면 해당 partition은 즉시 중단(break)
            # -----------------------------
            for tp, records in polled.items():
                # offset 순으로 안전하게 정렬
                records = sorted(records, key=lambda r: r.offset)

                partition_ok = True  # "연속 처리"가 깨지면 False로 전환

                for msg in records:
                    if not running or not partition_ok:
                        break

                    # 1) raw_text 만들기 (원본 그대로 파일에 저장하려고)
                    raw_bytes = msg.value
                    try:
                        raw_text = raw_bytes.decode("utf-8") if isinstance(raw_bytes, (bytes, bytearray)) else str(raw_bytes)
                    except Exception:
                        raw_text = str(raw_bytes)

                    # 2) ✅ 원본 파일 저장을 먼저 한다
                    # - DB가 실패해도 raw는 남는다(유실 방지)
                    # - 다음 재처리 때 파일은 이미 있으니 저장은 스킵되고 DB만 재시도
                    try:
                        saved_path = raw_sink.save_if_absent(
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                            raw_text=raw_text,
                        )
                    except Exception as e:
                        print(
                            f"[consumer] raw file save failed | {msg.topic}:{msg.partition}:{msg.offset} | {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        # 파일 저장 실패한 상태에서 뒤 offset 커밋하면 유실 위험 -> partition 중단
                        partition_ok = False
                        time.sleep(1)
                        break

                    # 3) ✅ DB 처리 (events insert + order_current upsert는 handler/repo가 담당)
                    handled = False
                    last_err: Optional[Exception] = None

                    for attempt in range(1, max_retries + 1):
                        try:
                            # 정상 JSON
                            event = json.loads(raw_text)
                            handled = handle_message(
                                event=event,
                                repo=repo,
                                kafka_meta={
                                    "topic": msg.topic,
                                    "partition": msg.partition,
                                    "offset": msg.offset,
                                    "timestamp": msg.timestamp,
                                    "raw_file_path": str(saved_path),
                                },
                            )
                            break

                        except json.JSONDecodeError as e:
                            # JSON 파싱 불가 => PARSE_ERROR로 events에 적재 (파이프라인 막지 않기)
                            handled = handle_message(
                                event={"__raw__": raw_text, "__error__": str(e)},
                                repo=repo,
                                kafka_meta={
                                    "topic": msg.topic,
                                    "partition": msg.partition,
                                    "offset": msg.offset,
                                    "timestamp": msg.timestamp,
                                    "raw_file_path": str(saved_path),
                                },
                            )
                            break

                        except Exception as e:
                            # DB 일시 장애 등으로 실패했을 가능성 -> 재시도
                            last_err = e
                            print(
                                f"[consumer] handle failed (attempt {attempt}/{max_retries})"
                                f" | {msg.topic}:{msg.partition}:{msg.offset} | {e}",
                                file=sys.stderr,
                                flush=True,
                            )
                            time.sleep(0.5)

                    if not handled:
                        # 처리 실패 상태에서 뒤 offset 커밋하면 유실 위험 -> partition 중단
                        if last_err is not None:
                            print(
                                f"[consumer] give up without commit (will reprocess)"
                                f" | {msg.topic}:{msg.partition}:{msg.offset}",
                                file=sys.stderr,
                                flush=True,
                            )
                        partition_ok = False
                        time.sleep(1)
                        break

                    # 4) ✅ 여기까지 왔으면 "파일 저장 + DB 반영"이 끝난 상태
                    # -> 그때만 offset 커밋 (at-least-once + 유실 방지)
                    try:
                        commit_one(consumer, msg.topic, msg.partition, msg.offset)
                    except Exception as e:
                        # 커밋 실패는 보통 "중복 처리"로 이어질 가능성이 큼(유실보단 낫다)
                        # 그래도 안전하게 partition 중단해서 다음 poll에서 재시도하도록 둠
                        print(
                            f"[consumer] commit failed | {msg.topic}:{msg.partition}:{msg.offset} | {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        partition_ok = False
                        time.sleep(0.5)
                        break

    finally:
        # 종료 시 리소스 정리
        print("[consumer] stop", flush=True)
        try:
            consumer.close()
        except Exception:
            pass
        try:
            repo.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())