from typing import TYPE_CHECKING, Any, Dict

from botocore.exceptions import BotoCoreError, ClientError

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__errors.error_permanent import ErrorPermanent
from services.__errors.error_transient import ErrorTransient

from .event_base import EventBase

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
else:
    DynamoDBServiceResource = Any
    Table = Any


class EventStoreClient:
    def __init__(self, dynamodb_resource: DynamoDBServiceResource, table_name: str):
        self._table: Table = dynamodb_resource.Table(table_name)

    # =====================================================
    # Raises an event in the event store
    # =====================================================
    def raise_event(self, event: EventBase):
        """
        Saves an event to DynamoDB, preventing duplicates.
        Raises:
            ErrorEventAlreadyRaised: If an event with the same idempotency key already exists.
            ErrorTransient: For transient errors (e.g., throttling).
            ErrorPermanent: For non-transient errors.
        """

        pk = f"EVENTS#{event.idempotencyKey}"
        sk = f"EVENT#{event.eventName}"
        item: Dict[str, Any] = {"pk": pk, "sk": sk, **event.model_dump()}

        try:
            self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
            # When successful, log the event storage
            print(f"SUCCESS: Raised event {sk} with key {pk}")

        except ClientError as e:
            # When a conditional check fails, it means the event already exists
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "ConditionalCheckFailedException":
                raise ErrorEventAlreadyRaisedException(e, event) from e

            # When throttling or throughput exceeded, it's a transient error
            elif error_code in ["ThrottlingException", "ProvisionedThroughputExceededException"]:
                print(f"ERROR: A transient AWS error occurred: {error_code}")
                raise ErrorTransient(e) from e

            # When other client errors occur, consider them permanent
            else:
                print(f"ERROR: A non-transient AWS error occurred: {error_code}")
                raise ErrorPermanent(e) from e

        # When other unknown errors occur, consider them transient
        except (BotoCoreError, Exception) as e:
            print(f"ERROR: An unknown, transient error occurred: {e}")
            raise ErrorTransient(e) from e

    # =====================================================
    # Lists events for a specific job
    # =====================================================
    def list_events_for_job(self, job_id: str) -> list[EventBase]:
        """
        Lists all events for a specific job by querying DynamoDB.
        Args:
            job_id: The ID of the job to list events for.
        Returns:
            A dictionary containing the list of events.
        Raises:
            ErrorTransient: For transient errors (e.g., throttling).
            ErrorPermanent: For non-transient errors.
        """
        pk = f"EVENTS#JOB_ID#{job_id}"

        try:
            response = self._table.query(
                KeyConditionExpression="pk = :pk AND begins_with(sk, :sk_prefix)",
                ExpressionAttributeValues={":pk": pk, ":sk_prefix": "EVENT#"},
            )
            items = response.get("Items", [])
            events = [EventBase.model_validate(item) for item in items]
            # Depending on the use case and number of events this access pattern might require a GSI or pagination.
            # Buts since this is a demo with a limited number of events, we sort them in memory.
            events.sort(key=lambda e: e.createdAt)
            print(f"SUCCESS: Retrieved {len(events)} events for job ID {job_id}")
            return events

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")

            # When throttling or throughput exceeded, it's a transient error
            if error_code in ["ThrottlingException", "ProvisionedThroughputExceededException"]:
                print(f"ERROR: A transient AWS error occurred: {error_code}")
                raise ErrorTransient(e) from e

            # When other client errors occur, consider them permanent
            else:
                print(f"ERROR: A non-transient AWS error occurred: {error_code}")
                raise ErrorPermanent(e) from e

        # When other unknown errors occur, consider them transient
        except (BotoCoreError, Exception) as e:
            print(f"ERROR: An unknown, transient error occurred: {e}")
            raise ErrorTransient(e) from e
