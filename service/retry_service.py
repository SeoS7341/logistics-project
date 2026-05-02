# service/retry_service.py

import json

from db.repository_retry import (
    save_retry_history
)

from db.repository_ingestion import (
    get_ingestion_job_by_trace_id,
    update_ingestion_job,
    increase_retry_count,
    get_raw_payload_by_trace_id
)

from db.repository_dlq import (
    insert_dead_letter
)

from service.ingestion_service import (
    process_raw_metadata
)

from common.constants.ingestion_status import (
    STAGE_FAILED,
    STATUS_FAIL,
    STATUS_SUCCESS,
    STATUS_DEAD,
    MAX_RETRY
)


def retry_failed_trace(
    db,
    trace_id: str
):

    job = get_ingestion_job_by_trace_id(
        db=db,
        trace_id=trace_id
    )

    if not job:
        raise ValueError(f"ingestion_job not found: {trace_id}")

    current_retry_count = job.retry_count

    # =========================================
    # MAX RETRY 초과
    # =========================================
    if current_retry_count >= MAX_RETRY:

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage=STAGE_FAILED,
            process_status=STATUS_DEAD,
            retry_count=current_retry_count,
            last_error="MAX RETRY EXCEEDED"
        )

        db.commit()

        save_retry_history(
            db=db,
            trace_id=trace_id,
            retry_status=STATUS_DEAD,
            retry_message="MAX RETRY EXCEEDED"
        )

        db.commit()

        return

    # =========================================
    # retry_count 증가
    # =========================================
    increase_retry_count(
        db=db,
        trace_id=trace_id
    )

    db.commit()

    # =========================================
    # raw_payload 조회
    # =========================================
    payload_result = get_raw_payload_by_trace_id(
        db=db,
        trace_id=trace_id
    )

    if not payload_result:
        raise ValueError("raw_payload not found.")

    payload = json.loads(payload_result[0])

    try:

        # =========================================
        # 실제 replay
        # =========================================
        process_raw_metadata(
            db=db,
            payload=payload
        )

        save_retry_history(
            db=db,
            trace_id=trace_id,
            retry_status=STATUS_SUCCESS,
            retry_message="RETRY SUCCESS"
        )

        db.commit()

    except Exception as e:

        updated_job = get_ingestion_job_by_trace_id(
            db=db,
            trace_id=trace_id
        )

        retry_count = updated_job.retry_count

        next_status = STATUS_FAIL

        if retry_count >= MAX_RETRY:
            next_status = STATUS_DEAD

        # =========================================
        # DEAD 전환시에만 상태 변경
        # =========================================
        if next_status == STATUS_DEAD:
            update_ingestion_job(
                db=db,
                trace_id=trace_id,
                process_stage=STAGE_FAILED,
                process_status=STATUS_DEAD,
                retry_count=retry_count,
                last_error=str(e)
            )

            db.commit()

        # =========================================
        # retry history 저장
        # =========================================
        save_retry_history(
            db=db,
            trace_id=trace_id,
            retry_status=next_status,
            retry_message=str(e)
        )

        db.commit()

        # =========================================
        # DLQ 적재
        # =========================================
        if next_status == STATUS_DEAD:

            insert_dead_letter(
                db=db,
                trace_id=trace_id,
                reason=str(e),
                raw_payload=payload
            )

            db.commit()

        raise e