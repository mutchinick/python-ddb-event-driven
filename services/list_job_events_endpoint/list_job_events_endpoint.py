import os
from typing import TYPE_CHECKING, Any, Dict

import boto3

from services.__events.event_store_client import EventStoreClient
from services.__http_helpers.http_response import HttpResponse

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import APIGatewayProxyEventV2
    from aws_lambda_typing.responses import APIGatewayProxyResponseV2
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
else:
    Context = Any
    APIGatewayProxyEventV2 = Dict[str, Any]
    APIGatewayProxyResponseV2 = Dict[str, Any]
    DynamoDBServiceResource = Any
    Table = Any


TABLE_NAME = os.environ.get("TABLE_NAME")
if not TABLE_NAME:
    raise ValueError("'TABLE_NAME' environment variable not set.")

dynamodb_client: DynamoDBServiceResource = boto3.resource("dynamodb")  # type: ignore
table: Table = dynamodb_client.Table(TABLE_NAME)


event_store_client = EventStoreClient(dynamodb_client, TABLE_NAME)


def handler(event: APIGatewayProxyEventV2, _context: Context) -> APIGatewayProxyResponseV2:
    """
    GET /jobs/{job_id}/events
    """

    path_params = event.get("pathParameters") or {}
    job_id = path_params.get("job_id")

    if not job_id or not job_id.strip():
        # When job_id is missing or empty, return 400 Bad Request
        print("ERROR: 'job_id' path parameter is required")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": "Missing 'job_id' in path"}
        )

    try:
        events_list = event_store_client.list_events_for_job(job_id)
        events_dicts = [event.model_dump() for event in events_list]
        return HttpResponse.api_gateway_responseV2(200, {"events": events_dicts})

    except Exception as e:
        # When other errors occur, return 500 Internal Server Error
        print(f"ERROR: Failed to list job events entity for job ID {job_id}: {e}")
        return HttpResponse.api_gateway_responseV2(
            500, {"message": "Internal Server Error", "details": str(e)}
        )
