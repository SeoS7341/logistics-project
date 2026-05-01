# service/trace_service.py

import uuid


def generate_trace_id():
    return str(uuid.uuid4())


def build_trace_response(trace_id: str, status: str):
    return {
        "status": status,
        "trace_id": trace_id
    }