# db/repository_event_history.py

import datetime


def save_event_history(
    db,
    trace_id,
    process_stage,
    process_status,
    message=None
):

    query = """
        INSERT INTO ingestion_event_history (
            trace_id,
            process_stage,
            process_status,
            message,
            created_at
        ) VALUES (
            :trace_id,
            :process_stage,
            :process_status,
            :message,
            :created_at
        )
    """

    db.execute(query, {
        "trace_id": trace_id,
        "process_stage": process_stage,
        "process_status": process_status,
        "message": message,
        "created_at": datetime.datetime.now()
    })