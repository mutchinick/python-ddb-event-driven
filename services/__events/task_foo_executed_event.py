from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from services.__events.event_base import EventBase


class TaskFooExecutedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class TaskFooExecutedEvent(EventBase):
    eventName: str = "TASK_FOO_EXECUTED_EVENT"
    eventData: TaskFooExecutedEventData

    @classmethod
    def from_data(cls, job_id: str, job_name: str, job_status: str) -> TaskFooExecutedEvent:
        """
        Factory method to create a TaskFooExecutedEvent from event data fields.
        Use this for manual event creation in APIs and workers.
        """
        event_data = TaskFooExecutedEventData(
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
