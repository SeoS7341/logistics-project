# db/repository_dlq.py

import json
import datetime


def insert_dead_letter(
    db,
    trace_id,
    reason,
    raw_payload
):

    query = """
        INSERT INTO dead_letter_queue (
            trace_id,
            reason,
            raw_payload,
            created_at
        ) VALUES (
            :trace_id,
            :reason,
            :raw_payload,
            :created_at
        )
    """

    db.execute(query, {
        "trace_id": trace_id,
        "reason": reason,
        "raw_payload": json.dumps(raw_payload, default=str),
        "created_at": datetime.datetime.now()
    })