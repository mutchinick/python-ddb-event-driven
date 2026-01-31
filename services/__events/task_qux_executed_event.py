from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from services.__events.event_base import EventBase


class TaskQuxExecutedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class TaskQuxExecutedEvent(EventBase):
    eventName: str = "TASK_QUX_EXECUTED_EVENT"
    eventData: TaskQuxExecutedEventData

    @classmethod
    def from_data(cls, job_id: str, job_name: str, job_status: str) -> TaskQuxExecutedEvent:
        """
        Factory method to create a TaskQuxExecutedEvent from event data fields.
        Use this for manual event creation in APIs and workers.
        """
        event_data = TaskQuxExecutedEventData(
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
