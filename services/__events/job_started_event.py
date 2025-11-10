from pydantic import BaseModel

from services.__events.event_base import EventBase


class JobStartedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class JobStartedEvent(EventBase):
    eventName: str = "JOB_STARTED_EVENT"
    eventData: JobStartedEventData
