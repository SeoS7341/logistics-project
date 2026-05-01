# db/repository_retry.py

import datetime


def save_retry_history(
    db,
    trace_id,
    retry_status,
    retry_message
):

    query = """
        INSERT INTO retry_history (
            trace_id,
            retry_status,
            retry_message,
            retried_at
        ) VALUES (
            :trace_id,
            :retry_status,
            :retry_message,
            :retried_at
        )
    """

    db.execute(query, {
        "trace_id": trace_id,
        "retry_status": retry_status,
        "retry_message": retry_message,
        "retried_at": datetime.datetime.now()
    })