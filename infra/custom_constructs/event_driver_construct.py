from aws_cdk import aws_events, aws_iam, aws_pipes
from constructs import Construct


class EventDriverConstruct(Construct):
    """
    Owns:
      - EventBridge Bus ("<base>-bus")
      - Pipe (DynamoDB Stream -> Event Bus, "<base>-pipe")
    Exposes: self.bus
    """

    bus: aws_events.EventBus
    pipe: aws_pipes.CfnPipe

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        base_name: str,
        table_stream_arn: str,
        source: str,
        detail_type: str,
        starting_position: str = "LATEST",
        batch_size: int = 1,
        maximum_batching_window_in_seconds: int = 0,
    ) -> None:
        super().__init__(scope, construct_id)

        # --- naming ---
        bus_name = f"{base_name}-bus"
        pipe_name = f"{base_name}-pipe"
        pipe_role_name = f"{base_name}-pipe-role"
        pipe_policy_name = f"{base_name}-pipe-policy"

        # --- resources ---
        self.bus = aws_events.EventBus(self, "EventBus", event_bus_name=bus_name)

        # --- pipe role & policy ---
        role = aws_iam.Role(
            self,
            "PipeRole",
            role_name=pipe_role_name,
            assumed_by=aws_iam.ServicePrincipal("pipes.amazonaws.com"),  # type: ignore
        )
        pipe_policy = aws_iam.Policy(
            self,
            "PipePolicy",
            policy_name=pipe_policy_name,
            statements=[
                aws_iam.PolicyStatement(
                    actions=[
                        "dynamodb:DescribeStream",
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:ListStreams",
                    ],
                    resources=[table_stream_arn],
                ),
                aws_iam.PolicyStatement(
                    actions=["events:PutEvents"],
                    resources=[self.bus.event_bus_arn],
                ),
            ],
        )
        role.attach_inline_policy(pipe_policy)

        self.pipe = aws_pipes.CfnPipe(
            self,
            "Pipe",
            name=pipe_name,
            role_arn=role.role_arn,
            source=table_stream_arn,
            target=self.bus.event_bus_arn,
            source_parameters=aws_pipes.CfnPipe.PipeSourceParametersProperty(
                dynamo_db_stream_parameters=aws_pipes.CfnPipe.PipeSourceDynamoDBStreamParametersProperty(
                    starting_position=starting_position,
                    batch_size=batch_size,
                    maximum_batching_window_in_seconds=maximum_batching_window_in_seconds,
                ),
            ),
            target_parameters=aws_pipes.CfnPipe.PipeTargetParametersProperty(
                event_bridge_event_bus_parameters=aws_pipes.CfnPipe.PipeTargetEventBridgeEventBusParametersProperty(
                    detail_type=detail_type,
                    source=source,
                ),
            ),
        )
