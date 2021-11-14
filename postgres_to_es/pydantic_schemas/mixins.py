from pydantic import BaseModel
from datetime import datetime
from uuid import UUID
from typing import Union


class IdMixin(BaseModel):
    """Mixin for id field"""
    id: Union[str, UUID]


class UpdatedMixin(BaseModel):
    """Mixin for update_at field"""
    updated_at: datetime
