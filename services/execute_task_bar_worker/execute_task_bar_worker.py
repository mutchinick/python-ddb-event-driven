import os
from typing import TYPE_CHECKING, Any, Dict

import boto3

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__events.event_base import EventBase
from services.__events.event_store_client import EventStoreClient
from services.__events.step_processed_event import StepProcessedEvent, StepProcessedEventData
from services.__events.task_bar_executed_event import TaskBarExecutedEvent

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
    SQS-triggered Lambda for StepProcessedEvent events.
    """
    for sqs_record in sqs_event["Records"]:
        try:
            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, StepProcessedEvent)
            print(f"INFO: StepProcessedEvent received with job ID: {incoming_event.idempotencyKey}")

        except Exception as e:
            # When parsing fails, log and remove the message from the queue because it's a poison message
            print(f"ERROR: Invalid SQS record: {e}. The record will be removed from the queue.")
            continue

        incoming_event_data: StepProcessedEventData = incoming_event.eventData

        event = TaskBarExecutedEvent.from_data(
            job_id=incoming_event_data.job_id,
            job_name=incoming_event_data.job_name,
            job_status="EXECUTED",
        )

        try:
            event_store_client.raise_event(event)
            # When successful, remove the message from the queue
            print(
                f"SUCCESS: TaskBarExecutedEvent raised for job ID: {incoming_event.idempotencyKey}"
            )

        except ErrorEventAlreadyRaisedException:
            # When the event was already raised, log and remove the message from the queue
            print(f"INFO: Idempotent request received for job ID: {incoming_event.idempotencyKey}")
            continue

        except Exception as e:
            # When other errors occur, log and re-raise to keep the message in the queue for retry
            record_id = sqs_record.get("messageId", "Unknown")
            print(f"ERROR: Error processing SQS record {record_id}: {e}")
            raise
