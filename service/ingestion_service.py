# service/ingestion_service.py

import time

from db.repository_ingestion import (
    save_unit_label,
    save_external_audit,
    upsert_ingestion_job
)

from service.trace_service import (
    generate_trace_id,
    build_trace_response
)

from service.container_validation_service import (
    validate_required_fields
)


def process_raw_metadata(db, payload: dict):

    start_time = time.time()

    trace_id = payload.get("trace_id") or generate_trace_id()

    system_name = payload.get("system_name", "UNKNOWN")

    is_success = 0
    status_code = "200"

    response_data = {}

    # =========================
    # ingestion_job 최초 생성
    # =========================
    upsert_ingestion_job(
        db=db,
        trace_id=trace_id,
        system_name=system_name,
        label_no=payload.get("label_no"),
        order_no=payload.get("order_no"),
        process_stage="INGEST_RECEIVED",
        process_status="PROCESSING",
        retry_count=0,
        last_error=None,
        raw_payload=payload
    )

    db.commit()

    try:

        # =========================
        # validation
        # =========================
        validate_required_fields(payload)

        # =========================
        # process stage update
        # =========================
        upsert_ingestion_job(
            db=db,
            trace_id=trace_id,
            system_name=system_name,
            label_no=payload.get("label_no"),
            order_no=payload.get("order_no"),
            process_stage="VALIDATION_COMPLETED",
            process_status="PROCESSING",
            retry_count=0,
            last_error=None,
            raw_payload=payload
        )

        db.commit()

        # =========================
        # unit_labels save
        # =========================
        save_unit_label(
            db=db,
            payload=payload,
            trace_id=trace_id
        )

        db.commit()

        # =========================
        # 성공 상태 반영
        # =========================
        upsert_ingestion_job(
            db=db,
            trace_id=trace_id,
            system_name=system_name,
            label_no=payload.get("label_no"),
            order_no=payload.get("order_no"),
            process_stage="UNIT_LABEL_SAVED",
            process_status="SUCCESS",
            retry_count=0,
            last_error=None,
            raw_payload=payload
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

        # =========================
        # 실패 상태 반영
        # =========================
        upsert_ingestion_job(
            db=db,
            trace_id=trace_id,
            system_name=system_name,
            label_no=payload.get("label_no"),
            order_no=payload.get("order_no"),
            process_stage="FAILED",
            process_status="FAIL",
            retry_count=0,
            last_error=str(e),
            raw_payload=payload
        )

        db.commit()

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