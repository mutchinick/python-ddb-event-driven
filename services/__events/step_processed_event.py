from pydantic import BaseModel

from services.__events.event_base import EventBase


class StepProcessedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class StepProcessedEvent(EventBase):
    eventName: str = "STEP_PROCESSED_EVENT"
    eventData: StepProcessedEventData
