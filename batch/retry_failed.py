# batch/retry_failed.py

import json

from db.connection import SessionLocal

from service.retry_service import retry_failed_trace
from service.ingestion_service import process_raw_metadata

from db.repository_ingestion import (
    get_failed_ingestion_jobs,
    update_ingestion_job
)


MAX_RETRY_COUNT = 3


def run_retry():

    db = SessionLocal()

    try:

        # =========================================
        # FAIL 상태 ingestion_job 조회
        # =========================================
        failed_jobs = get_failed_ingestion_jobs(db)

        if not failed_jobs:
            print("[RETRY] No failed ingestion jobs found.")
            return

        for row in failed_jobs:

            trace_id = row[0]
            retry_count = row[1]
            raw_payload = row[2]

            # =========================================
            # retry 제한
            # =========================================
            if retry_count >= MAX_RETRY_COUNT:

                print(
                    f"[RETRY SKIP] {trace_id} "
                    f"/ max retry exceeded ({retry_count})"
                )

                continue

            try:

                # =========================================
                # RETRYING 상태 변경
                # =========================================
                update_ingestion_job(
                    db=db,
                    trace_id=trace_id,
                    process_stage="RETRY",
                    process_status="RETRYING",
                    retry_count=retry_count + 1
                )

                db.commit()

                # =========================================
                # payload 복원
                # =========================================
                payload = json.loads(raw_payload)

                # =========================================
                # 실제 재처리 수행
                # =========================================
                result = process_raw_metadata(
                    db=db,
                    payload=payload
                )

                # =========================================
                # 성공 상태 업데이트
                # =========================================
                update_ingestion_job(
                    db=db,
                    trace_id=trace_id,
                    process_stage="COMPLETE",
                    process_status="SUCCESS",
                    retry_count=retry_count + 1,
                    last_error=None
                )

                db.commit()

                # =========================================
                # retry_history 저장
                # =========================================
                retry_failed_trace(
                    db=db,
                    trace_id=trace_id,
                    retry_message="Auto retry success",
                    retry_status="SUCCESS"
                )

                print(
                    f"[RETRY SUCCESS] "
                    f"{trace_id}"
                )

            except Exception as e:

                db.rollback()

                # =========================================
                # 실패 상태 업데이트
                # =========================================
                update_ingestion_job(
                    db=db,
                    trace_id=trace_id,
                    process_stage="RETRY",
                    process_status="FAIL",
                    retry_count=retry_count + 1,
                    last_error=str(e)
                )

                db.commit()

                # =========================================
                # retry_history 저장
                # =========================================
                retry_failed_trace(
                    db=db,
                    trace_id=trace_id,
                    retry_message=str(e),
                    retry_status="FAIL"
                )

                print(
                    f"[RETRY FAIL] "
                    f"{trace_id} / {e}"
                )

    finally:
        db.close()


if __name__ == "__main__":
    run_retry()