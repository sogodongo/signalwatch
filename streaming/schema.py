from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class Transaction(BaseModel):
    transaction_id:  str
    user_id:         str
    amount:          float
    currency:        str
    merchant_name:   str
    merchant_category: str
    country:         str
    city:            str
    timestamp:       str
    is_anomaly:      bool = False
    anomaly_type:    Optional[str] = None

    def to_kafka_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> "Transaction":
        return cls.model_validate_json(data.decode("utf-8"))


class AnomalyEvent(BaseModel):
    anomaly_id:      str
    transaction:     Transaction
    window_stats:    dict
    anomalies:       list[dict]
    combined_severity: str
    detected_at:     str

    def to_kafka_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> "AnomalyEvent":
        return cls.model_validate_json(data.decode("utf-8"))
