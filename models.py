from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

# --- Level 6: Internal Details ---
class VehicleLocation(BaseModel):
    longitude: Optional[str] = Field(alias="Longitude")
    latitude: Optional[str] = Field(alias="Latitude")

    @field_validator("longitude", "latitude", mode="before")
    @classmethod
    def allow_blank_strings(cls, v):
        """Converts blank empty strings or nulls from the API into None."""
        if v == "" or v is None:
            return None
        return str(v)

class MonitoredCall(BaseModel):
    stop_point_name: str = Field(alias="StopPointName")
    destination_display: str = Field(alias="DestinationDisplay")
    aimed_arrival_time: datetime = Field(alias="AimedArrivalTime")
    expected_arrival_time: Optional[datetime] = Field(alias="ExpectedArrivalTime")

# --- Level 5: The Vehicle's Journey Core ---
class MonitoredVehicleJourney(BaseModel):
    line_ref: str = Field(alias="LineRef")
    line_name: str = Field(alias="PublishedLineName")
    direction: str = Field(alias="DirectionRef")
    origin_name: str = Field(alias="OriginName")
    destination_name: str = Field(alias="DestinationName")
    vehicle_ref: Optional[str] = Field(alias="VehicleRef")
    location: Optional[VehicleLocation] = Field(alias="VehicleLocation")
    monitored_call: MonitoredCall = Field(alias="MonitoredCall")

# --- Level 4: Wrapper for individual stop visits ---
class MonitoredStopVisit(BaseModel):
    recorded_at: datetime = Field(alias="RecordedAtTime")
    monitoring_ref: str = Field(alias="MonitoringRef")
    journey: MonitoredVehicleJourney = Field(alias="MonitoredVehicleJourney")

# --- Level 3: Delivery Manifest ---
class StopMonitoringDelivery(BaseModel):
    version: str
    status: bool = Field(alias="Status")
    stop_visits: List[MonitoredStopVisit] = Field(alias="MonitoredStopVisit")

# --- Level 2: Top Core Payload ---
class ServiceDelivery(BaseModel):
    response_timestamp: datetime = Field(alias="ResponseTimestamp")
    producer_ref: str = Field(alias="ProducerRef")
    status: bool = Field(alias="Status")
    delivery: StopMonitoringDelivery = Field(alias="StopMonitoringDelivery")

# --- Level 1: Root Container ---
class TransitAPIResponse(BaseModel):
    service_delivery: ServiceDelivery = Field(alias="ServiceDelivery")
