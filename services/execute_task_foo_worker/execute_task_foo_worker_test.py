import json
from unittest.mock import MagicMock, patch

import pytest

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__errors.error_transient import ErrorTransient
from services.__events.event_base import EventBase
from services.__events.step_processed_event import StepProcessedEvent
from services.__events.task_foo_executed_event import TaskFooExecutedEvent, TaskFooExecutedEventData
from services.execute_task_foo_worker.execute_task_foo_worker import handler

"""
************************************************************
* Mock services
************************************************************
"""


def build_mock_event_store_client_succeeds():
    mock_client = MagicMock()
    mock_client.raise_event = MagicMock(return_value=None)
    return mock_client


def build_mock_event_store_client_raises(error: Exception):
    mock_client = MagicMock()
    mock_client.raise_event = MagicMock(side_effect=error)
    return mock_client


def build_mock_sqs_record_with_step_processed_event():
    """Builds a mock SQS record containing a StepProcessedEvent in EventBridge format."""
    step_processed_event = StepProcessedEvent.from_data(
        job_id="JOB-123",
        job_name="Test Job",
        job_status="PROCESSING",
    )

    # Simulate EventBridge SQS record structure
    ddb_record = {
        "dynamodb": {
            "NewImage": {
                "idempotencyKey": {"S": step_processed_event.idempotencyKey},
                "eventName": {"S": step_processed_event.eventName},
                "createdAt": {"S": step_processed_event.createdAt},
                "eventData": {
                    "M": {
                        "job_id": {"S": step_processed_event.eventData.job_id},
                        "job_name": {"S": step_processed_event.eventData.job_name},
                        "job_status": {"S": step_processed_event.eventData.job_status},
                    }
                },
            }
        }
    }

    eventbridge_event = {
        "detail": ddb_record,
    }

    return {
        "messageId": "test-message-id",
        "body": json.dumps(eventbridge_event),
    }


def build_mock_sqs_record_invalid():
    """Builds an invalid SQS record."""
    return {
        "messageId": "test-message-id",
        "body": "invalid json",
    }


def build_mock_sqs_record_missing_body():
    """Builds an SQS record missing the body."""
    return {
        "messageId": "test-message-id",
    }


class TestExecuteTaskFooWorker:
    """
    ************************************************************
    * Test edge cases - Invalid SQS records
    ************************************************************
    """

    def test_continues_if_sqs_record_is_invalid_json(self):
        sqs_event = {"Records": [build_mock_sqs_record_invalid()]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client"):
            # Should not raise, should continue processing
            handler(sqs_event, MagicMock())

    def test_continues_if_sqs_record_missing_body(self):
        sqs_event = {"Records": [build_mock_sqs_record_missing_body()]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client"):
            # Should not raise, should continue processing
            handler(sqs_event, MagicMock())

    def test_continues_if_sqs_record_has_invalid_structure(self):
        sqs_event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({"invalid": "structure"}),
                }
            ]
        }

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client"):
            # Should not raise, should continue processing
            handler(sqs_event, MagicMock())

    """
    ************************************************************
    * Test internal logic - Event reconstitution
    ************************************************************
    """

    def test_calls_from_eventbridge_sqs_record_with_step_processed_event(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            with patch.object(
                EventBase, "from_eventbridge_sqs_record", wraps=EventBase.from_eventbridge_sqs_record
            ) as mock_from_record:
                handler(sqs_event, MagicMock())

                mock_from_record.assert_called_once()
                # Verify it was called with StepProcessedEvent
                call_args = mock_from_record.call_args
                assert call_args[0][1] == StepProcessedEvent

    def test_reconstitutes_step_processed_event_correctly(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Verify the event was reconstituted correctly
            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, StepProcessedEvent)
            assert isinstance(incoming_event, StepProcessedEvent)
            assert incoming_event.eventData.job_id == "JOB-123"

    """
    ************************************************************
    * Test internal logic - Event creation
    ************************************************************
    """

    def test_creates_task_foo_executed_event_with_correct_data(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Verify raise_event was called
            assert mock_client.raise_event.called

            # Verify the event passed to raise_event is a TaskFooExecutedEvent
            call_args = mock_client.raise_event.call_args[0][0]
            assert isinstance(call_args, TaskFooExecutedEvent)
            assert call_args.eventName == "TASK_FOO_EXECUTED_EVENT"
            assert call_args.eventData.job_id == "JOB-123"
            assert call_args.eventData.job_name == "Test Job"
            assert call_args.eventData.job_status == "EXECUTED"

    def test_uses_correct_idempotency_key_format(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Verify the idempotency key has the correct format
            call_args = mock_client.raise_event.call_args[0][0]
            assert call_args.idempotencyKey == "JOB_ID#JOB-123"

    def test_calls_event_store_client_raise_event_single_time(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            mock_client.raise_event.assert_called_once()

    def test_processes_multiple_sqs_records(self):
        sqs_record1 = build_mock_sqs_record_with_step_processed_event()
        sqs_record2 = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record1, sqs_record2]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            assert mock_client.raise_event.call_count == 2

    """
    ************************************************************
    * Test error handling - ErrorEventAlreadyRaisedException
    ************************************************************
    """

    def test_continues_if_error_event_already_raised_exception(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        mock_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(
                side_effect=ErrorEventAlreadyRaisedException(Exception("Duplicate"), mock_event)
            )

            # Should not raise, should continue
            handler(sqs_event, MagicMock())

    """
    ************************************************************
    * Test error handling - Transient errors
    ************************************************************
    """

    def test_raises_if_transient_error_occurs(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(side_effect=ErrorTransient(Exception("Network error")))

            # Should re-raise transient errors
            with pytest.raises(ErrorTransient):
                handler(sqs_event, MagicMock())

    def test_raises_if_unknown_error_occurs(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(side_effect=RuntimeError("Unknown error"))

            # Should re-raise unknown errors (treated as transient by default)
            with pytest.raises(RuntimeError):
                handler(sqs_event, MagicMock())

    """
    ************************************************************
    * Test expected results - Successful processing
    ************************************************************
    """

    def test_processes_step_processed_event_successfully(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            # Should not raise
            handler(sqs_event, MagicMock())

            # Verify TaskFooExecutedEvent was created and raised
            assert mock_client.raise_event.called
            call_args = mock_client.raise_event.call_args[0][0]
            assert isinstance(call_args, TaskFooExecutedEvent)
            assert call_args.eventData.job_status == "EXECUTED"

    def test_creates_task_foo_executed_event_with_correct_job_status(self):
        sqs_record = build_mock_sqs_record_with_step_processed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.execute_task_foo_worker.execute_task_foo_worker.event_store_client") as mock_client:
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            call_args = mock_client.raise_event.call_args[0][0]
            task_event_data: TaskFooExecutedEventData = call_args.eventData
            assert task_event_data.job_status == "EXECUTED"
