from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from services.__events.event_base import EventBase


class AllTasksCompletedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class AllTasksCompletedEvent(EventBase):
    eventName: str = "ALL_TASKS_COMPLETED_EVENT"
    eventData: AllTasksCompletedEventData

    @classmethod
    def from_data(cls, job_id: str, job_name: str, job_status: str) -> AllTasksCompletedEvent:
        """
        Factory method to create an AllTasksCompletedEvent from event data fields.
        Use this for manual event creation in APIs and workers.
        """
        event_data = AllTasksCompletedEventData(
            job_id=job_id,
            job_name=job_name,
            job_status=job_status,
        )

        createdAt = datetime.now().isoformat()
        idempotencyKey = f"JOB_ID#{job_id}"

        return cls(
            idempotencyKey=idempotencyKey,
            createdAt=createdAt,
            eventData=event_data,
        )
