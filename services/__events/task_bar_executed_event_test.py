from datetime import datetime

import pytest
from pydantic import ValidationError

from services.__events.task_bar_executed_event import TaskBarExecutedEvent, TaskBarExecutedEventData


class TestTaskBarExecutedEvent:
    """
    ************************************************************
    * Test event data validation
    ************************************************************
    """

    def test_raises_validation_error_if_job_id_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEventData(job_id=None, job_name="Test", job_status="EXECUTED")

    def test_does_not_raise_if_job_id_is_empty(self):
        # Pydantic allows empty strings by default
        data = TaskBarExecutedEventData(job_id="", job_name="Test", job_status="EXECUTED")
        assert data.job_id == ""

    def test_raises_validation_error_if_job_name_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEventData(job_id="JOB-123", job_name=None, job_status="EXECUTED")

    def test_does_not_raise_if_job_name_is_empty(self):
        # Pydantic allows empty strings by default
        data = TaskBarExecutedEventData(job_id="JOB-123", job_name="", job_status="EXECUTED")
        assert data.job_name == ""

    def test_raises_validation_error_if_job_status_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEventData(job_id="JOB-123", job_name="Test", job_status=None)

    def test_does_not_raise_if_job_status_is_empty(self):
        # Pydantic allows empty strings by default
        data = TaskBarExecutedEventData(job_id="JOB-123", job_name="Test", job_status="")
        assert data.job_status == ""

    def test_raises_validation_error_if_missing_required_fields(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEventData(job_id="JOB-123")

    """
    ************************************************************
    * Test factory method (from_data)
    ************************************************************
    """

    def test_from_data_creates_event_with_valid_data(self):
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        assert isinstance(event, TaskBarExecutedEvent)
        assert event.idempotencyKey == "JOB_ID#JOB-123"
        assert event.createdAt is not None
        assert isinstance(event.eventData, TaskBarExecutedEventData)
        assert event.eventData.job_id == "JOB-123"
        assert event.eventData.job_name == "Test Job"
        assert event.eventData.job_status == "EXECUTED"

    def test_from_data_generates_correct_idempotency_key(self):
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-456", job_name="Test", job_status="EXECUTED"
        )
        assert event.idempotencyKey == "JOB_ID#JOB-456"

    def test_from_data_generates_created_at(self):
        before = datetime.now().isoformat()
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        after = datetime.now().isoformat()
        assert before <= event.createdAt <= after

    def test_from_data_raises_validation_error_if_job_id_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEvent.from_data(job_id=None, job_name="Test", job_status="EXECUTED")

    def test_from_data_raises_validation_error_if_job_name_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEvent.from_data(job_id="JOB-123", job_name=None, job_status="EXECUTED")

    def test_from_data_raises_validation_error_if_job_status_is_none(self):
        with pytest.raises(ValidationError):
            TaskBarExecutedEvent.from_data(job_id="JOB-123", job_name="Test", job_status=None)

    """
    ************************************************************
    * Test event properties
    ************************************************************
    """

    def test_event_has_correct_event_name(self):
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="EXECUTED"
        )
        assert event.eventName == "TASK_BAR_EXECUTED_EVENT"

    def test_event_data_has_correct_structure(self):
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        assert event.eventData.job_id == "JOB-123"
        assert event.eventData.job_name == "Test Job"
        assert event.eventData.job_status == "EXECUTED"

    """
    ************************************************************
    * Test event serialization
    ************************************************************
    """

    def test_event_can_be_serialized_to_dict(self):
        event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        event_dict = event.model_dump()
        assert event_dict["eventName"] == "TASK_BAR_EXECUTED_EVENT"
        assert event_dict["idempotencyKey"] == "JOB_ID#JOB-123"
        assert event_dict["createdAt"] is not None
        assert event_dict["eventData"]["job_id"] == "JOB-123"
        assert event_dict["eventData"]["job_name"] == "Test Job"
        assert event_dict["eventData"]["job_status"] == "EXECUTED"

    def test_event_can_be_reconstituted_from_dict(self):
        original_event = TaskBarExecutedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="EXECUTED"
        )
        event_dict = original_event.model_dump()
        reconstituted_event = TaskBarExecutedEvent.model_validate(event_dict)
        assert reconstituted_event.eventName == original_event.eventName
        assert reconstituted_event.idempotencyKey == original_event.idempotencyKey
        assert reconstituted_event.createdAt == original_event.createdAt
        assert reconstituted_event.eventData.job_id == original_event.eventData.job_id
        assert reconstituted_event.eventData.job_name == original_event.eventData.job_name
        assert reconstituted_event.eventData.job_status == original_event.eventData.job_status
