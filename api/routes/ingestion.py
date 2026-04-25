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
    current_trace_id = str(uuid.uuid4())
    system_name = payload.get("system_name", "UNKNOWN")
    
    is_success = 0
    status_code = "200"
    response_data = {}

    try:
        # 1. 도메인 로직 처리 (unit_labels 적재)
        # DDL에 따라 label_no, order_no는 필수입니다.
        label_no = payload.get("label_no")
        order_no = payload.get("order_no")

        if not label_no or not order_no:
            status_code = "400"
            raise ValueError("label_no and order_no are mandatory fields.")

        # Upsert 쿼리: 필수 컬럼을 채우고 나머지는 raw_metadata에 위임
        upsert_query = """
            INSERT INTO unit_labels (
                label_no, 
                order_no, 
                container_id, 
                product_barcode, 
                cust_code, 
                raw_metadata, 
                created_at
            ) VALUES (
                :l_no, :o_no, :c_id, :p_barcode, :cust, :raw, :now
            )
            ON DUPLICATE KEY UPDATE 
                order_no = :o_no,
                container_id = :c_id,
                product_barcode = :p_barcode,
                cust_code = :cust,
                raw_metadata = :raw
        """
        
        db.execute(upsert_query, {
            "l_no": label_no,
            "o_no": order_no,
            "c_id": payload.get("container_id"),      # NULL 허용
            "p_barcode": payload.get("product_barcode"), # NULL 허용
            "cust": payload.get("cust_code"),         # NULL 허용
            "raw": json.dumps(payload),               # 가상 컬럼(v_mall_id 등) 추출원
            "now": datetime.datetime.now()
        })

        db.commit()
        is_success = 1
        response_data = {"status": "success", "trace_id": current_trace_id}

    except Exception as e:
        db.rollback()
        is_success = 0
        # ValueError는 400, 나머지는 500으로 처리
        status_code = "400" if isinstance(e, ValueError) else "500"
        response_data = {"status": "error", "detail": str(e), "trace_id": current_trace_id}

    finally:
        # 2. Audit 로그 기록 (DDL 준수)
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
            "trace": current_trace_id,
            "now": datetime.datetime.now()
        })
        db.commit()

    if is_success == 0:
        raise HTTPException(status_code=int(status_code), detail=response_data["detail"])

    return response_data