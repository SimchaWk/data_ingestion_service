from typing import List, Optional
from pydantic import BaseModel, field_validator
from datetime import datetime


class TerrorEvent(BaseModel):
    event_id: str
    event_date: datetime
    country: str
    city: str
    region: Optional[str] = None
    province_or_state: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    num_killed: Optional[float] = None
    num_terrorist_killed: Optional[float] = None
    num_wounded: Optional[float] = None
    num_terrorist_wounded: Optional[float] = None
    total_casualties: Optional[float] = None
    num_perpetrators: Optional[int] = None
    num_perpetrators_captured: Optional[int] = None
    attack_types: List[str] = List
    target_details: List[str] = List
    terror_groups: List[str] = List
    summary: Optional[str] = None
    description: Optional[str] = None
    data_source: str

    @classmethod
    @field_validator("latitude", "longitude")
    def validate_coordinates(cls, value, field):
        if value is not None and not (-180 <= value <= 180):
            raise ValueError(f"{field.name} must be between -180 and 180")
        return value

    @classmethod
    @field_validator("event_date")
    def validate_date(cls, value):
        if value > datetime.now():
            raise ValueError("The event date cannot be in the future")
        return value
