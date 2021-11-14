from pydantic import BaseModel
from datetime import datetime


class IdMixin(BaseModel):
    """Mixin for id field"""
    id: str


class UpdatedMixin(BaseModel):
    """Mixin for update_at field"""
    updated_at: datetime
