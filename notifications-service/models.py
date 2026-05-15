from pydantic import BaseModel

class Notification(BaseModel):
    order_id: int
    product_id: int
    message: str

class ErrorNotification(BaseModel):
    order_id: int
    product_id: int
    timestamp: str
    error_reason: str
    message: str
