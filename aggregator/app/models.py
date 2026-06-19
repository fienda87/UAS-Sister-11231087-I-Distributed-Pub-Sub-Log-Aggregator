from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel, Field


class EventIn(BaseModel):
    topic: str = Field(min_length=1, max_length=200)
    event_id: str = Field(min_length=1, max_length=200)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=200)
    payload: Dict[str, Any] = Field(default_factory=dict)


class PublishResponse(BaseModel):
    received: int
    inserted: int
    duplicates: int


class TopicCount(BaseModel):
    topic: str
    done: int


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[TopicCount]
    queue: dict
    uptime_seconds: int
