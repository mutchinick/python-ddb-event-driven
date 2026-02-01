from pydantic import BaseModel

from services.__events.event_base import EventBase


class JobCreatedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class JobCreatedEvent(EventBase):
    eventName: str = "JOB_CREATED_EVENT"
    eventData: JobCreatedEventData
