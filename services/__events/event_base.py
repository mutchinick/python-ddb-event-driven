import json
from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar

from boto3.dynamodb.types import TypeDeserializer
from pydantic import BaseModel

if TYPE_CHECKING:
    from aws_lambda_typing.events.sqs import SQSMessage
else:
    SQSMessage = Dict[str, Any]


TEvent = TypeVar("TEvent", bound="EventBase")


class EventBase(BaseModel):
    idempotencyKey: str
    eventName: str
    createdAt: str
    eventData: Any

    @staticmethod
    def from_dynamodb_record(ddb_record: Dict[str, Any], model: Type[TEvent]) -> TEvent:
        """
        Parses a single DynamoDB Stream record (like one from an
        EventBridge Pipe) by unmarshalling the 'NewImage'.
        """
        try:
            new_image = ddb_record["dynamodb"]["NewImage"]
            deserializer = TypeDeserializer()
            dictionary = {k: deserializer.deserialize(v) for k, v in new_image.items()}
            return model(**dictionary)

        except (KeyError, TypeError) as e:
            # When the expected keys are not found or types mismatch
            raise ValueError(f"Failed to parse DynamoDB payload: {e}") from e

    @staticmethod
    def from_eventbridge_sqs_record(sqs_record: SQSMessage, model: Type[TEvent]) -> TEvent:
        """
        Parses an SQS record where the body is an EventBridge event
        that itself contains a *single, unwrapped* DDB record.

        Flow: SQS Record -> Body (string) -> EventBridge -> Detail (DDB Record) -> NewImage
        """
        try:
            eb_payload = json.loads(sqs_record.get("body", "{}"))
            ddb_record = eb_payload["detail"]
            return EventBase.from_dynamodb_record(ddb_record, model)

        except (KeyError, IndexError, json.JSONDecodeError, TypeError) as e:
            # When the expected keys are not found or types mismatch
            raise ValueError(f"Failed to parse SQS-EB-DDB payload: {e}") from e
