from fastapi import FastAPI
from api.routes import ingestion

app = FastAPI(title="Logistics Data Pipeline API")

# 라우터 등록
app.include_router(ingestion.router)

@app.get("/")
async def root():
    return {"message": "Logistics API is running"}