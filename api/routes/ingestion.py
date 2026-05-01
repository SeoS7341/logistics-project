# api/routes/ingestion.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from service.ingestion_service import process_raw_metadata

router = APIRouter(prefix="/ingest", tags=["Ingestion"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/raw-metadata")
async def post_raw_metadata(payload: dict, db: Session = Depends(get_db)):
    try:
        result = process_raw_metadata(db, payload)
        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))