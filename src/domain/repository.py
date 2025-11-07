"""Domain entities for GitHub repositories."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Repository:
    """Immutable repository entity."""
    
    id: str
    name: str
    owner: str
    full_name: str
    stars: int
    url: str
    created_at: datetime
    updated_at: datetime

