from dataclasses import dataclass
from typing import Dict, Optional, cast

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_apigatewayv2,
    aws_apigatewayv2_integrations,
    aws_lambda,
    aws_logs,
)
from constructs import Construct


@dataclass
class ApiEndpointProps:
    base_name: str  # e.g., "create-jobs-endpoint"
    method: str  # "GET" | "POST" | ...
    path: str  # "/jobs" or "/jobs/{job_id}/"
    code_asset_path: str
    handler: str  # "module.function"
    runtime: aws_lambda.Runtime = aws_lambda.Runtime.PYTHON_3_12
    memory_mb: int = 256
    timeout_sec: int = 20
    environment: Optional[Dict[str, str]] = None
    log_retention_days: aws_logs.RetentionDays = aws_logs.RetentionDays.ONE_WEEK


class ApiEndpointConstruct(Construct):
    """
    One endpoint:
      - Explicit CloudWatch Log Group
      - Lambda (name: "<base>-fn")
      - Integration
      - Route on provided HttpApi
    """

    lambda_function: aws_lambda.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        http_api: aws_apigatewayv2.IHttpApi,
        props: ApiEndpointProps,
    ) -> None:
        super().__init__(scope, construct_id)

        # --- naming ---
        function_name = f"{props.base_name}-fn"

        # --- resources ---
        log_group = aws_logs.LogGroup(
            self,
            "EndpointLogGroup",
            log_group_name=f"/aws/lambda/{function_name}",
            retention=props.log_retention_days,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fn = aws_lambda.Function(
            self,
            "EndpointFn",
            function_name=function_name,
            runtime=props.runtime,
            handler=props.handler,
            code=aws_lambda.Code.from_asset(props.code_asset_path),
            memory_size=props.memory_mb,
            timeout=Duration.seconds(props.timeout_sec),
            environment={**(props.environment or {})},
            log_group=log_group,
        )

        integration = aws_apigatewayv2_integrations.HttpLambdaIntegration(
            "LambdaIntegration", handler=cast(aws_lambda.IFunction, fn)
        )

        aws_apigatewayv2.HttpRoute(
            self,
            "Route",
            http_api=http_api,
            route_key=aws_apigatewayv2.HttpRouteKey.with_(
                props.path, method=aws_apigatewayv2.HttpMethod(props.method)
            ),
            integration=integration,
        )

        self.lambda_function = fn
