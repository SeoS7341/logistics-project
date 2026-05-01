# service/retry_service.py

import datetime

from db.repository_retry import (
    save_retry_history
)

from db.repository_ingestion import (
    get_ingestion_job_by_trace_id,
    update_ingestion_job
)


# =========================================
# retry 이력 저장 + ingestion_job 상태 반영
# =========================================
def retry_failed_trace(
    db,
    trace_id: str,
    retry_message: str,
    retry_status: str = "SUCCESS"
):

    # =========================================
    # 현재 ingestion_job 조회
    # =========================================
    job = get_ingestion_job_by_trace_id(
        db=db,
        trace_id=trace_id
    )

    retry_count = 0

    if job:
        retry_count = job[4] or 0

    # =========================================
    # retry_history 저장
    # =========================================
    save_retry_history(
        db=db,
        trace_id=trace_id,
        retry_status=retry_status,
        retry_message=retry_message
    )

    # =========================================
    # ingestion_job 상태 업데이트
    # =========================================
    if retry_status == "SUCCESS":

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage="RETRY",
            process_status="SUCCESS",
            retry_count=retry_count,
            last_error=None
        )

    else:

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage="RETRY",
            process_status="FAIL",
            retry_count=retry_count,
            last_error=retry_message
        )

    db.commit()