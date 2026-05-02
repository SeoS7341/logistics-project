# service/ingestion_service.py

import time

from db.repository_ingestion import (
    save_unit_label,
    save_external_audit,
    create_ingestion_job,
    update_ingestion_job
)

from service.trace_service import (
    generate_trace_id,
    build_trace_response
)

from service.container_validation_service import (
    validate_required_fields
)

from service.master_validation_service import (
    validate_master_data
)

from common.constants.ingestion_status import (
    STAGE_RECEIVED,
    STAGE_VALIDATED,
    STAGE_MASTER_CHECKED,
    STAGE_UNIT_LABEL_SAVED,
    STAGE_COMPLETED,
    STAGE_FAILED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_FAIL
)


def process_raw_metadata(db, payload: dict):

    start_time = time.time()

    trace_id = payload.get("trace_id") or generate_trace_id()

    system_name = payload.get("system_name", "UNKNOWN")

    is_success = 0
    status_code = "200"

    response_data = {}

    try:

        # =========================================
        # STEP 1. ingestion_job 생성
        # =========================================
        create_ingestion_job(
            db=db,
            trace_id=trace_id,
            system_name=system_name,
            payload=payload,
            process_stage=STAGE_RECEIVED,
            process_status=STATUS_RUNNING
        )

        db.commit()

        # =========================================
        # STEP 2. validation
        # =========================================
        validate_required_fields(payload)

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage=STAGE_VALIDATED,
            process_status=STATUS_RUNNING
        )

        db.commit()

        # =========================================
        # STEP 3. master validation
        # =========================================
        validate_master_data(
            db=db,
            payload=payload
        )

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage=STAGE_MASTER_CHECKED,
            process_status=STATUS_RUNNING
        )

        db.commit()

        # =========================================
        # STEP 4. unit_labels 저장
        # =========================================
        save_unit_label(
            db=db,
            payload=payload,
            trace_id=trace_id
        )

        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage=STAGE_UNIT_LABEL_SAVED,
            process_status=STATUS_RUNNING
        )

        db.commit()

        # =========================================
        # STEP 5. 완료 처리
        # =========================================
        update_ingestion_job(
            db=db,
            trace_id=trace_id,
            process_stage=STAGE_COMPLETED,
            process_status=STATUS_SUCCESS
        )

        db.commit()

        is_success = 1

        response_data = build_trace_response(
            trace_id=trace_id,
            status="success"
        )

    except Exception as e:

        db.rollback()

        is_success = 0

        if isinstance(e, ValueError):
            status_code = "400"
        else:
            status_code = "500"

        try:

            update_ingestion_job(
                db=db,
                trace_id=trace_id,
                process_stage=STAGE_FAILED,
                process_status=STATUS_FAIL,
                last_error=str(e)
            )

            db.commit()

        except Exception:
            db.rollback()

        response_data = {
            "status": "error",
            "detail": str(e),
            "trace_id": trace_id
        }

        raise e

    finally:

        latency_ms = int((time.time() - start_time) * 1000)

        save_external_audit(
            db=db,
            system_name=system_name,
            request_payload=payload,
            response_payload=response_data,
            status_code=status_code,
            is_success=is_success,
            latency_ms=latency_ms,
            trace_id=trace_id
        )

        db.commit()

    return response_data