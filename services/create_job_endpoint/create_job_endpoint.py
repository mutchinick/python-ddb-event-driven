import os
from typing import TYPE_CHECKING, Any, Dict

import boto3
from pydantic import BaseModel, ValidationError

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__events.event_store_client import EventStoreClient
from services.__events.job_created_event import JobCreatedEvent
from services.__http_helpers.http_response import HttpResponse

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import APIGatewayProxyEventV2
    from aws_lambda_typing.responses import APIGatewayProxyResponseV2
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    Context = Any
    APIGatewayProxyEventV2 = Dict[str, Any]
    APIGatewayProxyResponseV2 = Dict[str, Any]
    DynamoDBServiceResource = Any


class IncomingCreateJobRequest(BaseModel):
    job_id: str
    job_name: str


dynamodb_client: DynamoDBServiceResource = boto3.resource("dynamodb")  # type: ignore

TABLE_NAME = os.environ.get("TABLE_NAME")
if not TABLE_NAME:
    raise ValueError("'TABLE_NAME' environment variable not set.")

event_store_client = EventStoreClient(dynamodb_client, TABLE_NAME)


def handler(event: APIGatewayProxyEventV2, _context: Context) -> APIGatewayProxyResponseV2:
    """
    API Gateway Lambda handler to create a new job.
    POST /jobs
    Body example:
      {
        "job_id": "JOB-123",
        "job_name": "Data Processing Job",
      }
    Args:
      event: APIGatewayProxyEventV2 - The API Gateway event payload.
      _context: Context - The Lambda execution context.
    Returns:
        APIGatewayProxyResponseV2 - The HTTP response.
        202 Accepted on success or if the event was already raised
        400 Bad Request on validation errors
        500 Internal Server Error on other failures
    Exceptions:
        Catches and handles ValidationError and JSONDecodeError for input validation.
    """

    body_str = event.get("body")
    if not body_str:
        # When body is missing, return 400 Bad Request
        print("ERROR: Request body is required")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": "Request body is required"}
        )

    try:
        incoming_create_job_request = IncomingCreateJobRequest.model_validate_json(body_str)

    # When validation fails, return 400 Bad Request
    except ValidationError as e:
        print(f"ERROR: Failed to validate IncomingCreateJobRequest: {e}")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": str(e)}
        )

    job_event = JobCreatedEvent.from_data(
        job_id=incoming_create_job_request.job_id,
        job_name=incoming_create_job_request.job_name,
        job_status="CREATED",
    )

    try:
        event_store_client.raise_event(job_event)
        # When successful return 202 Accepted
        print(f"SUCCESS: JobCreatedEvent raised for job ID: {job_event.idempotencyKey}")
        return HttpResponse.api_gateway_responseV2(
            202, {"message": "Accepted", "result": incoming_create_job_request.model_dump()}
        )

    except ErrorEventAlreadyRaisedException:
        # When the event was already raised, return 202 Accepted as well
        print(f"INFO: Idempotent request received for job ID: {job_event.idempotencyKey}")
        return HttpResponse.api_gateway_responseV2(
            202, {"message": "Accepted", "result": incoming_create_job_request.model_dump()}
        )

    except Exception as e:
        # When other errors occur, return 500 Internal Server Error
        print(f"ERROR: Failed to raise JobCreatedEvent: {e}")
        return HttpResponse.api_gateway_responseV2(
            500, {"message": "Internal Server Error", "details": str(e)}
        )
