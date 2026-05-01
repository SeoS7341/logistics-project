# db/repository_ingestion.py

import json
import datetime


# =========================================
# unit_labels UPSERT
# =========================================
def save_unit_label(db, payload: dict, trace_id: str):

    upsert_query = """
        INSERT INTO unit_labels (
            label_no,
            order_no,
            container_id,
            product_code,
            cust_code,
            raw_metadata,
            trace_id,
            created_at
        ) VALUES (
            :l_no,
            :o_no,
            :c_id,
            :p_code,
            :cust,
            :raw,
            :trace,
            :now
        )
        ON DUPLICATE KEY UPDATE
            order_no = :o_no,
            container_id = :c_id,
            product_code = :p_code,
            cust_code = :cust,
            raw_metadata = :raw,
            trace_id = :trace
    """

    db.execute(upsert_query, {
        "l_no": payload.get("label_no"),
        "o_no": payload.get("order_no"),
        "c_id": payload.get("container_id"),
        "p_code": payload.get("product_code"),
        "cust": payload.get("cust_code"),
        "raw": json.dumps(payload),
        "trace": trace_id,
        "now": datetime.datetime.now()
    })


# =========================================
# external_api_audit 저장
# =========================================
def save_external_audit(
    db,
    system_name,
    request_payload,
    response_payload,
    status_code,
    is_success,
    latency_ms,
    trace_id
):

    audit_query = """
        INSERT INTO external_api_audit (
            system_name,
            request_payload,
            response_payload,
            status_code,
            is_success,
            latency_ms,
            trace_id,
            created_at
        ) VALUES (
            :sys,
            :req,
            :res,
            :status,
            :success,
            :latency,
            :trace,
            :now
        )
    """

    db.execute(audit_query, {
        "sys": system_name,
        "req": json.dumps(request_payload),
        "res": json.dumps(response_payload),
        "status": status_code,
        "success": is_success,
        "latency": latency_ms,
        "trace": trace_id,
        "now": datetime.datetime.now()
    })


# =========================================
# ingestion_job UPSERT
# =========================================
def upsert_ingestion_job(
    db,
    trace_id,
    system_name,
    label_no,
    order_no,
    process_stage,
    process_status,
    retry_count,
    last_error,
    raw_payload
):

    query = """
        INSERT INTO ingestion_job (
            trace_id,
            system_name,
            label_no,
            order_no,
            process_stage,
            process_status,
            retry_count,
            last_error,
            raw_payload,
            created_at,
            updated_at
        ) VALUES (
            :trace_id,
            :system_name,
            :label_no,
            :order_no,
            :process_stage,
            :process_status,
            :retry_count,
            :last_error,
            :raw_payload,
            :created_at,
            :updated_at
        )
        ON DUPLICATE KEY UPDATE
            process_stage = :process_stage,
            process_status = :process_status,
            retry_count = :retry_count,
            last_error = :last_error,
            raw_payload = :raw_payload,
            updated_at = :updated_at
    """

    now = datetime.datetime.now()

    db.execute(query, {
        "trace_id": trace_id,
        "system_name": system_name,
        "label_no": label_no,
        "order_no": order_no,
        "process_stage": process_stage,
        "process_status": process_status,
        "retry_count": retry_count,
        "last_error": last_error,
        "raw_payload": json.dumps(raw_payload),
        "created_at": now,
        "updated_at": now
    })


# =========================================
# ingestion_job 상태 업데이트
# =========================================
def update_ingestion_job(
    db,
    trace_id,
    process_stage,
    process_status,
    retry_count=None,
    last_error=None
):

    query = """
        UPDATE ingestion_job
        SET
            process_stage = :process_stage,
            process_status = :process_status,
            updated_at = :updated_at
    """

    params = {
        "process_stage": process_stage,
        "process_status": process_status,
        "updated_at": datetime.datetime.now(),
        "trace_id": trace_id
    }

    if retry_count is not None:
        query += """
            , retry_count = :retry_count
        """
        params["retry_count"] = retry_count

    if last_error is not None:
        query += """
            , last_error = :last_error
        """
        params["last_error"] = last_error

    query += """
        WHERE trace_id = :trace_id
    """

    db.execute(query, params)


# =========================================
# ingestion_job 조회
# =========================================
def get_ingestion_job_by_trace_id(
    db,
    trace_id
):

    query = """
        SELECT
            id,
            trace_id,
            system_name,
            label_no,
            order_no,
            process_stage,
            process_status,
            retry_count,
            last_error,
            raw_payload,
            created_at,
            updated_at
        FROM ingestion_job
        WHERE trace_id = :trace_id
    """

    result = db.execute(query, {
        "trace_id": trace_id
    })

    return result.fetchone()


# =========================================
# 실패 ingestion_job 조회
# =========================================
def get_failed_ingestion_jobs(db):

    query = """
        SELECT
            trace_id,
            retry_count,
            raw_payload,
            process_stage,
            process_status,
            last_error
        FROM ingestion_job
        WHERE process_status = 'FAIL'
    """

    result = db.execute(query)

    return result.fetchall()