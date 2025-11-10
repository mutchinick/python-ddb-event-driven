import json
import os
from typing import Any, Dict

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    POST /orders
    Body example:
      {
        "pk": "ORDER#123",
        "sk": "v1",
        "EventName": "OrderCreated",
        "Payload": {"customerId":"C-1","total":123.45}
      }
    """
    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError:
        return _resp(400, {"error": "Invalid JSON body"})

    pk = body.get("pk")
    sk = body.get("sk", "v1")
    event_name = body.get("EventName", "OrderCreated")
    payload = body.get("Payload", {})

    if not pk:
        return _resp(400, {"error": "Missing 'pk'"})

    item = {
        "pk": pk,
        "sk": sk,
        "EventName": event_name,
        "Payload": json.dumps(payload),
    }
    table.put_item(Item=item)

    return _resp(201, {"message": "created", "item": item})


def _resp(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }
