from pydantic import BaseModel
from typing import Any, Dict, Optional

class IngestionPayload(BaseModel):
    system_name: str  # 예: 'FOODPANG', 'NEULPUM', 'CJ'
    data_type: str    # 예: 'label', 'order'
    data: Dict[str, Any]  # 실제 데이터 본체