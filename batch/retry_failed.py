# batch/retry_failed.py

from db.connection import SessionLocal

from service.retry_service import (
    retry_failed_trace
)

from db.repository_ingestion import (
    get_failed_ingestion_jobs
)


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

            try:

                # =========================================
                # retry orchestration 수행
                # =========================================
                retry_failed_trace(
                    db=db,
                    trace_id=trace_id
                )

                print(
                    f"[RETRY COMPLETED] {trace_id}"
                )

            except Exception as e:

                print(
                    f"[RETRY FAIL] {trace_id} / {e}"
                )

    finally:

        db.close()


if __name__ == "__main__":

    run_retry()