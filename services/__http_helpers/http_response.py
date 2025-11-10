import json
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from aws_lambda_typing.responses import APIGatewayProxyResponseV2
else:
    APIGatewayProxyResponseV2 = Dict[str, Any]


class HttpResponse:
    @staticmethod
    def api_gateway_responseV2(status: int, body: Dict[str, Any]) -> APIGatewayProxyResponseV2:
        """
        Builds an API Gateway proxy response with CORS headers.
        """
        return {
            "statusCode": status,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,PATCH,DELETE",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            },
            "body": json.dumps(body),
        }
