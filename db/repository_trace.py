# db/repository_trace.py

def get_ingestion_job_trace(db, trace_id):

    query = """
        SELECT
            trace_id,
            process_stage,
            process_status,
            retry_count,
            last_error,
            created_at,
            updated_at
        FROM ingestion_job
        WHERE trace_id = :trace_id
    """

    result = db.execute(query, {
        "trace_id": trace_id
    })

    return result.fetchone()


def get_retry_history_trace(db, trace_id):

    query = """
        SELECT
            retry_status,
            retry_message,
            retried_at
        FROM retry_history
        WHERE trace_id = :trace_id
        ORDER BY retried_at ASC
    """

    result = db.execute(query, {
        "trace_id": trace_id
    })

    return result.fetchall()


def get_external_audit_trace(db, trace_id):

    query = """
        SELECT
            status_code,
            is_success,
            latency_ms,
            created_at
        FROM external_api_audit
        WHERE trace_id = :trace_id
        ORDER BY created_at ASC
    """

    result = db.execute(query, {
        "trace_id": trace_id
    })

    return result.fetchall()