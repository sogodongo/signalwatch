from pydantic import BaseModel
from typing import Optional


class InjectAnomalyRequest(BaseModel):
    user_id:          str = "user_001"
    amount:           float = 850.0
    country:          str = "KE"
    merchant_category: str = "electronics"
    anomaly_type:     str = "amount_zscore"

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id":           "user_002",
                "amount":            2500.0,
                "country":           "NG",
                "merchant_category": "electronics",
                "anomaly_type":      "amount_zscore",
            }
        }
    }


class AnomalyResponse(BaseModel):
    anomaly_id:        str
    user_id:           str
    amount:            float
    country:           str
    combined_severity: str
    threat_level:      Optional[str] = None
    confidence:        Optional[float] = None
    final_action:      Optional[str] = None
    status:            Optional[str] = None
    detected_at:       Optional[str] = None
