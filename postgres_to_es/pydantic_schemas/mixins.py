from datetime import datetime

from pydantic import BaseModel


class IdMixin(BaseModel):
    """Mixin for id field"""

    id: str


class UpdatedMixin(BaseModel):
    """Mixin for update_at field"""

    updated_at: datetime
