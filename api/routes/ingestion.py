from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import json
import datetime
import uuid
import time

from db.connection import SessionLocal 

router = APIRouter(prefix="/ingest", tags=["Ingestion"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/raw-metadata")
async def post_raw_metadata(payload: dict, db: Session = Depends(get_db)):
    start_time = time.time()

    # 🔥 핵심: 외부 trace_id 사용
    trace_id = payload.get("trace_id") or str(uuid.uuid4())

    system_name = payload.get("system_name", "UNKNOWN")
    
    is_success = 0
    status_code = "200"
    response_data = {}

    try:
        # =========================
        # 1. 필수값 검증
        # =========================
        label_no = payload.get("label_no")
        order_no = payload.get("order_no")

        if not label_no or not order_no:
            raise ValueError("label_no and order_no are mandatory fields.")

        # =========================
        # 2. unit_labels UPSERT
        # 🔥 trace_id 포함
        # =========================
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
                :l_no, :o_no, :c_id, :p_code, :cust, :raw, :trace, :now
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
            "l_no": label_no,
            "o_no": order_no,
            "c_id": payload.get("container_id"),
            "p_code": payload.get("product_code"),
            "cust": payload.get("cust_code"),
            "raw": json.dumps(payload),
            "trace": trace_id,  # 🔥 핵심
            "now": datetime.datetime.now()
        })

        db.commit()

        is_success = 1
        response_data = {
            "status": "success",
            "trace_id": trace_id  # 🔥 그대로 반환
        }

    except Exception as e:
        db.rollback()

        is_success = 0
        status_code = "400" if isinstance(e, ValueError) else "500"

        response_data = {
            "status": "error",
            "detail": str(e),
            "trace_id": trace_id
        }

    finally:
        # =========================
        # 3. Audit 로그
        # 🔥 동일 trace_id 사용
        # =========================
        latency_ms = int((time.time() - start_time) * 1000)

        audit_query = """
            INSERT INTO external_api_audit (
                system_name, request_payload, response_payload, 
                status_code, is_success, latency_ms, trace_id, created_at
            ) VALUES (
                :sys, :req, :res, :status, :success, :latency, :trace, :now
            )
        """

        db.execute(audit_query, {
            "sys": system_name,
            "req": json.dumps(payload),
            "res": json.dumps(response_data),
            "status": status_code,
            "success": is_success,
            "latency": latency_ms,
            "trace": trace_id,  # 🔥 동일
            "now": datetime.datetime.now()
        })

        db.commit()

    if is_success == 0:
        raise HTTPException(
            status_code=int(status_code),
            detail=response_data["detail"]
        )

    return response_data