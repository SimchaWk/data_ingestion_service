from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class EventRecord(BaseModel):
    event_date: datetime = Field(..., description="The date of the event, must not be null")
    country: str = Field(..., description="The country where the event occurred")
    city: str = Field(..., description="The city where the event occurred")
    region: Optional[str] = Field(None, description="The region where the event occurred")
    province_or_state: Optional[str] = Field(None, description="The province or state of the event")
    latitude: Optional[float] = Field(None, description="Latitude of the event location")
    longitude: Optional[float] = Field(None, description="Longitude of the event location")
    num_killed: Optional[float] = Field(None, ge=0, description="Number of people killed, must be non-negative")
    num_terrorist_killed: Optional[float] = Field(None, ge=0,
                                                  description="Number of terrorists killed, must be non-negative")
    num_wounded: Optional[float] = Field(None, ge=0, description="Number of people wounded, must be non-negative")
    num_terrorist_wounded: Optional[float] = Field(None, ge=0,
                                                   description="Number of terrorists wounded, must be non-negative")
    total_casualties: Optional[float] = Field(None, ge=0,
                                              description="Total number of casualties, must be non-negative")
    num_perpetrators: Optional[int] = Field(None, ge=0,
                                            description="Number of perpetrators involved, must be non-negative")
    num_perpetrators_captured: Optional[int] = Field(None, ge=0,
                                                     description="Number of perpetrators captured, must be non-negative")
    attack_type_1: Optional[str] = Field(None, description="Primary type of attack")
    attack_type_2: Optional[str] = Field(None, description="Secondary type of attack")
    attack_type_3: Optional[str] = Field(None, description="Tertiary type of attack")
    target_type_1: Optional[str] = Field(None, description="Primary type of target")
    target_subtype_1: Optional[str] = Field(None, description="Subtype of primary target")
    target_type_2: Optional[str] = Field(None, description="Secondary type of target")
    target_subtype_2: Optional[str] = Field(None, description="Subtype of secondary target")
    target_type_3: Optional[str] = Field(None, description="Tertiary type of target")
    target_subtype_3: Optional[str] = Field(None, description="Subtype of tertiary target")
    terror_group_name: Optional[str] = Field(None, description="Name of the terrorist group responsible")
    terror_group_subname: Optional[str] = Field(None, description="Subname of the terrorist group responsible")
    secondary_terror_group_name: Optional[str] = Field(None,
                                                       description="Name of the secondary terrorist group responsible")
    secondary_terror_group_subname: Optional[str] = Field(None,
                                                          description="Subname of the secondary terrorist group responsible")
    tertiary_terror_group_name: Optional[str] = Field(None,
                                                      description="Name of the tertiary terrorist group responsible")
    tertiary_terror_group_subname: Optional[str] = Field(None,
                                                         description="Subname of the tertiary terrorist group responsible")
    description: Optional[str] = Field(None, description="Description of the event")
    data_source: str = Field(..., description="Source of the data")

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

# Example usage:
# df = rename_event_record_columns(df)
# normalized_df = normalize_data(df)
# validated_event = EventRecord(
#     event_date="1970-07-07",
#     country="United States",
#     city="New York City",
#     region="North America",
#     province_or_state="New York",
#     latitude=40.697132,
#     longitude=-73.931351,
#     num_killed=0,
#     num_terrorist_killed=0,
#     num_wounded=0,
#     num_terrorist_wounded=0,
#     total_casualties=0,
#     num_perpetrators=1,
#     num_perpetrators_captured=0,
#     attack_type_1="Bombing/Explosion",
#     target_type_1="Government",
#     terror_group_name="Unknown",
#     description="An explosive device was found",
#     data_source="matched"
# )
