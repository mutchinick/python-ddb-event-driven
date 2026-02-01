from datetime import datetime

import pytest
from pydantic import ValidationError

from services.__events.all_tasks_completed_event import (
    AllTasksCompletedEvent,
    AllTasksCompletedEventData,
)


class TestAllTasksCompletedEvent:
    """
    ************************************************************
    * Test event data validation
    ************************************************************
    """

    def test_raises_validation_error_if_job_id_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEventData(job_id=None, job_name="Test", job_status="COMPLETED")

    def test_does_not_raise_if_job_id_is_empty(self):
        # Pydantic allows empty strings by default
        data = AllTasksCompletedEventData(job_id="", job_name="Test", job_status="COMPLETED")
        assert data.job_id == ""

    def test_raises_validation_error_if_job_name_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEventData(job_id="JOB-123", job_name=None, job_status="COMPLETED")

    def test_does_not_raise_if_job_name_is_empty(self):
        # Pydantic allows empty strings by default
        data = AllTasksCompletedEventData(job_id="JOB-123", job_name="", job_status="COMPLETED")
        assert data.job_name == ""

    def test_raises_validation_error_if_job_status_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEventData(job_id="JOB-123", job_name="Test", job_status=None)

    def test_does_not_raise_if_job_status_is_empty(self):
        # Pydantic allows empty strings by default
        data = AllTasksCompletedEventData(job_id="JOB-123", job_name="Test", job_status="")
        assert data.job_status == ""

    def test_raises_validation_error_if_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEventData(job_id="JOB-123")

    """
    ************************************************************
    * Test factory method (from_data)
    ************************************************************
    """

    def test_from_data_creates_event_with_valid_data(self):
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="COMPLETED"
        )
        assert isinstance(event, AllTasksCompletedEvent)
        assert event.idempotencyKey == "JOB_ID#JOB-123"
        assert event.createdAt is not None
        assert isinstance(event.eventData, AllTasksCompletedEventData)
        assert event.eventData.job_id == "JOB-123"
        assert event.eventData.job_name == "Test Job"
        assert event.eventData.job_status == "COMPLETED"

    def test_from_data_generates_correct_idempotency_key(self):
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-456", job_name="Test", job_status="COMPLETED"
        )
        assert event.idempotencyKey == "JOB_ID#JOB-456"

    def test_from_data_generates_created_at(self):
        before = datetime.now().isoformat()
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="COMPLETED"
        )
        after = datetime.now().isoformat()
        assert before <= event.createdAt <= after

    def test_from_data_raises_validation_error_if_job_id_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEvent.from_data(job_id=None, job_name="Test", job_status="COMPLETED")

    def test_from_data_raises_validation_error_if_job_name_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEvent.from_data(job_id="JOB-123", job_name=None, job_status="COMPLETED")

    def test_from_data_raises_validation_error_if_job_status_is_none(self):
        with pytest.raises(ValidationError):
            AllTasksCompletedEvent.from_data(job_id="JOB-123", job_name="Test", job_status=None)

    """
    ************************************************************
    * Test event properties
    ************************************************************
    """

    def test_event_has_correct_event_name(self):
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test", job_status="COMPLETED"
        )
        assert event.eventName == "ALL_TASKS_COMPLETED_EVENT"

    def test_event_data_has_correct_structure(self):
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="COMPLETED"
        )
        assert event.eventData.job_id == "JOB-123"
        assert event.eventData.job_name == "Test Job"
        assert event.eventData.job_status == "COMPLETED"

    """
    ************************************************************
    * Test event serialization
    ************************************************************
    """

    def test_event_can_be_serialized_to_dict(self):
        event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="COMPLETED"
        )
        event_dict = event.model_dump()
        assert event_dict["eventName"] == "ALL_TASKS_COMPLETED_EVENT"
        assert event_dict["idempotencyKey"] == "JOB_ID#JOB-123"
        assert event_dict["createdAt"] is not None
        assert event_dict["eventData"]["job_id"] == "JOB-123"
        assert event_dict["eventData"]["job_name"] == "Test Job"
        assert event_dict["eventData"]["job_status"] == "COMPLETED"

    def test_event_can_be_reconstituted_from_dict(self):
        original_event = AllTasksCompletedEvent.from_data(
            job_id="JOB-123", job_name="Test Job", job_status="COMPLETED"
        )
        event_dict = original_event.model_dump()
        reconstituted_event = AllTasksCompletedEvent.model_validate(event_dict)
        assert reconstituted_event.eventName == original_event.eventName
        assert reconstituted_event.idempotencyKey == original_event.idempotencyKey
        assert reconstituted_event.createdAt == original_event.createdAt
        assert reconstituted_event.eventData.job_id == original_event.eventData.job_id
        assert reconstituted_event.eventData.job_name == original_event.eventData.job_name
        assert reconstituted_event.eventData.job_status == original_event.eventData.job_status
