import json
import os
from typing import Any, Dict

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    GET /orders/{pk}/{sk}
    """
    path_params = event.get("pathParameters") or {}
    pk = path_params.get("pk")
    sk = path_params.get("sk")

    if not pk or not sk:
        return _resp(400, {"error": "Missing 'pk' or 'sk' in path"})

    resp = table.get_item(Key={"pk": pk, "sk": sk})
    item = resp.get("Item")
    if not item:
        return _resp(404, {"error": "Not found"})

    return _resp(200, {"item": item})


def _resp(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }
