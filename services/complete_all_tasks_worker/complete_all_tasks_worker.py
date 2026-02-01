import os
from typing import TYPE_CHECKING, Any, Dict

import boto3

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__events.all_tasks_completed_event import AllTasksCompletedEvent
from services.__events.event_base import EventBase
from services.__events.event_store_client import EventStoreClient
from services.__events.task_bar_executed_event import TaskBarExecutedEvent
from services.__events.task_foo_executed_event import TaskFooExecutedEvent
from services.__events.task_qux_executed_event import TaskQuxExecutedEvent

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    Context = Any
    SQSEvent = Dict[str, Any]
    DynamoDBServiceResource = Any


dynamodb_client: DynamoDBServiceResource = boto3.resource("dynamodb")  # type: ignore

TABLE_NAME = os.environ.get("TABLE_NAME")
if not TABLE_NAME:
    raise ValueError("'TABLE_NAME' environment variable not set.")

event_store_client = EventStoreClient(dynamodb_client, TABLE_NAME)


def handler(sqs_event: SQSEvent, _context: Context) -> None:
    """
    SQS-triggered Lambda for task execution events.
    Listens to TASK_FOO_EXECUTED_EVENT, TASK_QUX_EXECUTED_EVENT, and TASK_BAR_EXECUTED_EVENT.
    Only creates ALL_TASKS_COMPLETED_EVENT when all three task events exist for the job.
    """
    for sqs_record in sqs_event["Records"]:
        try:
            # Try to reconstitute as any of the three task events
            incoming_event = None
            for event_class in [TaskFooExecutedEvent, TaskQuxExecutedEvent, TaskBarExecutedEvent]:
                try:
                    incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, event_class)
                    print(
                        f"INFO: {event_class.__name__} received with job ID: {incoming_event.idempotencyKey}"
                    )
                    break
                except Exception:
                    continue

            if incoming_event is None:
                # When parsing fails, log and remove the message from the queue because it's a poison message
                print("ERROR: Invalid SQS record: Could not parse as any task event. The record will be removed from the queue.")
                continue

        except Exception as e:
            # When parsing fails, log and remove the message from the queue because it's a poison message
            print(f"ERROR: Invalid SQS record: {e}. The record will be removed from the queue.")
            continue

        # Extract job_id from the incoming event
        job_id = incoming_event.eventData.job_id
        job_name = incoming_event.eventData.job_name

        # Check if all three task events exist for this job
        try:
            all_events = event_store_client.list_events_for_job(job_id)
        except Exception as e:
            # When querying fails, log and re-raise to keep the message in the queue for retry
            record_id = sqs_record.get("messageId", "Unknown")
            print(f"ERROR: Error querying events for job {job_id} (record {record_id}): {e}")
            raise

        # Check if all three task events exist
        task_foo_exists = any(
            e.eventName == "TASK_FOO_EXECUTED_EVENT" for e in all_events
        )
        task_qux_exists = any(
            e.eventName == "TASK_QUX_EXECUTED_EVENT" for e in all_events
        )
        task_bar_exists = any(
            e.eventName == "TASK_BAR_EXECUTED_EVENT" for e in all_events
        )

        # Only proceed if all three events exist
        if not (task_foo_exists and task_qux_exists and task_bar_exists):
            print(
                f"INFO: Not all tasks completed for job {job_id}. Waiting for remaining tasks."
            )
            # Remove message from queue since we've processed it (even though we're not creating the completion event yet)
            continue

        # Check if ALL_TASKS_COMPLETED_EVENT already exists (idempotency check)
        all_tasks_completed_exists = any(
            e.eventName == "ALL_TASKS_COMPLETED_EVENT" for e in all_events
        )

        if all_tasks_completed_exists:
            print(
                f"INFO: AllTasksCompletedEvent already exists for job {job_id}. Skipping creation."
            )
            continue

        # Create ALL_TASKS_COMPLETED_EVENT
        event = AllTasksCompletedEvent.from_data(
            job_id=job_id,
            job_name=job_name,
            job_status="COMPLETED",
        )

        try:
            event_store_client.raise_event(event)
            # When successful, remove the message from the queue
            print(f"SUCCESS: AllTasksCompletedEvent raised for job ID: {job_id}")

        except ErrorEventAlreadyRaisedException:
            # When the event was already raised (race condition), log and remove the message from the queue
            print(f"INFO: Idempotent request received for job ID: {job_id}")
            continue

        except Exception as e:
            # When other errors occur, log and re-raise to keep the message in the queue for retry
            record_id = sqs_record.get("messageId", "Unknown")
            print(f"ERROR: Error processing SQS record {record_id}: {e}")
            raise
