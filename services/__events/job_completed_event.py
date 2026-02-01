from pydantic import BaseModel

from services.__events.event_base import EventBase


class JobCompletedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class JobCompletedEvent(EventBase):
    eventName: str = "JOB_COMPLETED_EVENT"
    eventData: JobCompletedEventData
