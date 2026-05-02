# service/trace_query_service.py

from db.repository_trace import (
    get_ingestion_job_trace,
    get_retry_history_trace,
    get_external_audit_trace
)


def get_trace_timeline(db, trace_id):

    ingestion_job = get_ingestion_job_trace(
        db=db,
        trace_id=trace_id
    )

    if not ingestion_job:
        raise ValueError(f"trace_id not found: {trace_id}")

    retry_histories = get_retry_history_trace(
        db=db,
        trace_id=trace_id
    )

    audit_histories = get_external_audit_trace(
        db=db,
        trace_id=trace_id
    )

    timeline = []

    timeline.append({
        "event_type": "INGESTION_JOB",
        "stage": ingestion_job.process_stage,
        "status": ingestion_job.process_status,
        "created_at": str(ingestion_job.created_at)
    })

    for retry in retry_histories:

        timeline.append({
            "event_type": "RETRY",
            "retry_status": retry.retry_status,
            "message": retry.retry_message,
            "created_at": str(retry.retried_at)
        })

    for audit in audit_histories:

        timeline.append({
            "event_type": "AUDIT",
            "status_code": audit.status_code,
            "is_success": audit.is_success,
            "latency_ms": audit.latency_ms,
            "created_at": str(audit.created_at)
        })

    return {
        "trace_id": ingestion_job.trace_id,
        "current_stage": ingestion_job.process_stage,
        "current_status": ingestion_job.process_status,
        "retry_count": ingestion_job.retry_count,
        "last_error": ingestion_job.last_error,
        "timeline": timeline
    }