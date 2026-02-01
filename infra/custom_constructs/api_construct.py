import json

from aws_cdk import RemovalPolicy, aws_apigatewayv2, aws_logs
from constructs import Construct


class ApiConstruct(Construct):
    """
    Owns a shared HTTP API.
    Exposes: self.http_api
    """

    http_api: aws_apigatewayv2.IHttpApi
    api_endpoint: str

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        base_name: str,
        log_retention_days: aws_logs.RetentionDays = aws_logs.RetentionDays.ONE_WEEK,
    ) -> None:
        super().__init__(scope, construct_id)

        # --- naming ---
        api_name = f"{base_name}-http-api"

        # --- resources ---
        log_group = aws_logs.LogGroup(
            self,
            "ApiLogGroup",
            log_group_name=f"/aws/apigateway/{api_name}",
            retention=log_retention_days,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.http_api = aws_apigatewayv2.HttpApi(
            self,
            "HttpApi",
            api_name=api_name,
            create_default_stage=False,
        )

        self.stage = aws_apigatewayv2.CfnStage(
            self,
            "DefaultStage",
            api_id=self.http_api.http_api_id,
            stage_name="$default",
            auto_deploy=True,
            access_log_settings=aws_apigatewayv2.CfnStage.AccessLogSettingsProperty(
                destination_arn=log_group.log_group_arn,
                format=json.dumps(
                    {
                        "requestId": "$context.requestId",
                        "ip": "$context.identity.sourceIp",
                        "caller": "$context.identity.caller",
                        "user": "$context.identity.user",
                        "requestTime": "$context.requestTime",
                        "httpMethod": "$context.httpMethod",
                        "resourcePath": "$context.resourcePath",
                        "status": "$context.status",
                        "protocol": "$context.protocol",
                        "responseLength": "$context.responseLength",
                    }
                ),
            ),
        )

        self.api_endpoint = self.http_api.url or ""
