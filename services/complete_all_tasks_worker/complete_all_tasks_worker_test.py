import json
from unittest.mock import MagicMock, patch

import pytest

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__errors.error_transient import ErrorTransient
from services.__events.all_tasks_completed_event import (
    AllTasksCompletedEvent,
)
from services.__events.event_base import EventBase
from services.__events.task_bar_executed_event import TaskBarExecutedEvent
from services.__events.task_foo_executed_event import TaskFooExecutedEvent
from services.__events.task_qux_executed_event import TaskQuxExecutedEvent
from services.complete_all_tasks_worker.complete_all_tasks_worker import handler

"""
************************************************************
* Mock services
************************************************************
"""


def build_mock_sqs_record_with_task_foo_executed_event():
    """Builds a mock SQS record containing a TaskFooExecutedEvent in EventBridge format."""
    task_event = TaskFooExecutedEvent.from_data(
        job_id="JOB-123",
        job_name="Test Job",
        job_status="EXECUTED",
    )

    ddb_record = {
        "dynamodb": {
            "NewImage": {
                "idempotencyKey": {"S": task_event.idempotencyKey},
                "eventName": {"S": task_event.eventName},
                "createdAt": {"S": task_event.createdAt},
                "eventData": {
                    "M": {
                        "job_id": {"S": task_event.eventData.job_id},
                        "job_name": {"S": task_event.eventData.job_name},
                        "job_status": {"S": task_event.eventData.job_status},
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


def build_mock_sqs_record_with_task_qux_executed_event():
    """Builds a mock SQS record containing a TaskQuxExecutedEvent in EventBridge format."""
    task_event = TaskQuxExecutedEvent.from_data(
        job_id="JOB-123",
        job_name="Test Job",
        job_status="EXECUTED",
    )

    ddb_record = {
        "dynamodb": {
            "NewImage": {
                "idempotencyKey": {"S": task_event.idempotencyKey},
                "eventName": {"S": task_event.eventName},
                "createdAt": {"S": task_event.createdAt},
                "eventData": {
                    "M": {
                        "job_id": {"S": task_event.eventData.job_id},
                        "job_name": {"S": task_event.eventData.job_name},
                        "job_status": {"S": task_event.eventData.job_status},
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


def build_mock_sqs_record_with_task_bar_executed_event():
    """Builds a mock SQS record containing a TaskBarExecutedEvent in EventBridge format."""
    task_event = TaskBarExecutedEvent.from_data(
        job_id="JOB-123",
        job_name="Test Job",
        job_status="EXECUTED",
    )

    ddb_record = {
        "dynamodb": {
            "NewImage": {
                "idempotencyKey": {"S": task_event.idempotencyKey},
                "eventName": {"S": task_event.eventName},
                "createdAt": {"S": task_event.createdAt},
                "eventData": {
                    "M": {
                        "job_id": {"S": task_event.eventData.job_id},
                        "job_name": {"S": task_event.eventData.job_name},
                        "job_status": {"S": task_event.eventData.job_status},
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


class TestCompleteAllTasksWorker:
    """
    ************************************************************
    * Test edge cases - Invalid SQS records
    ************************************************************
    """

    def test_continues_if_sqs_record_is_invalid_json(self):
        sqs_event = {"Records": [build_mock_sqs_record_invalid()]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client"):
            handler(sqs_event, MagicMock())

    """
    ************************************************************
    * Test internal logic - Event reconstitution
    ************************************************************
    """

    def test_reconstitutes_task_foo_executed_event_correctly(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, TaskFooExecutedEvent)
            assert isinstance(incoming_event, TaskFooExecutedEvent)
            assert incoming_event.eventData.job_id == "JOB-123"

    def test_reconstitutes_task_qux_executed_event_correctly(self):
        sqs_record = build_mock_sqs_record_with_task_qux_executed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, TaskQuxExecutedEvent)
            assert isinstance(incoming_event, TaskQuxExecutedEvent)
            assert incoming_event.eventData.job_id == "JOB-123"

    def test_reconstitutes_task_bar_executed_event_correctly(self):
        sqs_record = build_mock_sqs_record_with_task_bar_executed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, TaskBarExecutedEvent)
            assert isinstance(incoming_event, TaskBarExecutedEvent)
            assert incoming_event.eventData.job_id == "JOB-123"

    """
    ************************************************************
    * Test internal logic - All tasks completion check
    ************************************************************
    """

    def test_does_not_create_all_tasks_completed_when_only_one_task_exists(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock only one task event exists
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[task_foo_event])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Should not call raise_event for AllTasksCompletedEvent
            mock_client.raise_event.assert_not_called()

    def test_does_not_create_all_tasks_completed_when_only_two_tasks_exist(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock only two task events exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[task_foo_event, task_qux_event])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Should not call raise_event for AllTasksCompletedEvent
            mock_client.raise_event.assert_not_called()

    def test_creates_all_tasks_completed_when_all_three_tasks_exist(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock all three task events exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_bar_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(
                return_value=[task_foo_event, task_qux_event, task_bar_event]
            )
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Should call raise_event for AllTasksCompletedEvent
            assert mock_client.raise_event.called
            call_args = mock_client.raise_event.call_args[0][0]
            assert isinstance(call_args, AllTasksCompletedEvent)
            assert call_args.eventName == "ALL_TASKS_COMPLETED_EVENT"
            assert call_args.eventData.job_id == "JOB-123"
            assert call_args.eventData.job_status == "COMPLETED"

    def test_does_not_create_duplicate_all_tasks_completed_event(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock all three task events and AllTasksCompletedEvent already exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_bar_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        all_tasks_completed_event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="COMPLETED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(
                return_value=[task_foo_event, task_qux_event, task_bar_event, all_tasks_completed_event]
            )
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Should not call raise_event since AllTasksCompletedEvent already exists
            mock_client.raise_event.assert_not_called()

    def test_calls_list_events_for_job_with_correct_job_id(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(return_value=[])
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            # Verify list_events_for_job was called with correct job_id
            mock_client.list_events_for_job.assert_called_once_with("JOB-123")

    """
    ************************************************************
    * Test error handling - ErrorEventAlreadyRaisedException
    ************************************************************
    """

    def test_continues_if_error_event_already_raised_exception(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock all three task events exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_bar_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        mock_event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="COMPLETED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(
                return_value=[task_foo_event, task_qux_event, task_bar_event]
            )
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

    def test_raises_if_list_events_for_job_fails_with_transient_error(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(side_effect=ErrorTransient(Exception("Network error")))

            # Should re-raise transient errors
            with pytest.raises(ErrorTransient):
                handler(sqs_event, MagicMock())

    def test_raises_if_raise_event_fails_with_transient_error(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock all three task events exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        task_bar_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(
                return_value=[task_foo_event, task_qux_event, task_bar_event]
            )
            mock_client.raise_event = MagicMock(side_effect=ErrorTransient(Exception("Network error")))

            # Should re-raise transient errors
            with pytest.raises(ErrorTransient):
                handler(sqs_event, MagicMock())

    """
    ************************************************************
    * Test expected results - Successful processing
    ************************************************************
    """

    def test_creates_all_tasks_completed_event_with_correct_data(self):
        sqs_record = build_mock_sqs_record_with_task_foo_executed_event()
        sqs_event = {"Records": [sqs_record]}

        # Mock all three task events exist
        task_foo_event = TaskFooExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        task_qux_event = TaskQuxExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        task_bar_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )

        with patch("services.complete_all_tasks_worker.complete_all_tasks_worker.event_store_client") as mock_client:
            mock_client.list_events_for_job = MagicMock(
                return_value=[task_foo_event, task_qux_event, task_bar_event]
            )
            mock_client.raise_event = MagicMock(return_value=None)

            handler(sqs_event, MagicMock())

            assert mock_client.raise_event.called
            call_args = mock_client.raise_event.call_args[0][0]
            assert isinstance(call_args, AllTasksCompletedEvent)
            assert call_args.eventData.job_id == "JOB-123"
            assert call_args.eventData.job_name == "Test Job"
            assert call_args.eventData.job_status == "COMPLETED"
