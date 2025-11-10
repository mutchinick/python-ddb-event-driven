from dataclasses import dataclass
from typing import Dict, List, Optional

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_events,
    aws_events_targets,
    aws_lambda,
    aws_lambda_event_sources,
    aws_logs,
    aws_sqs,
)
from constructs import Construct


@dataclass
class WorkerProps:
    base_name: str  # e.g., "start-job-worker"
    match_event_names: List[str]
    source: str
    detail_type: str
    code_asset_path: str  # folder with code or zip file
    handler: str  # "module.function"
    runtime: aws_lambda.Runtime = aws_lambda.Runtime.PYTHON_3_12
    memory_mb: int = 256
    lambda_timeout_sec: int = 30
    visibility_timeout_sec: int = 30
    retention_days: int = 4
    enable_dlq: bool = True
    environment: Optional[Dict[str, str]] = None
    log_retention_days: aws_logs.RetentionDays = aws_logs.RetentionDays.ONE_WEEK


class WorkerConstruct(Construct):
    """
    Creates:
      - SQS queue         (name: "<base>-queue")
      - optional DLQ      (name: "<base>-dlq")
      - CloudWatch Log Group
      - Lambda function   (name: "<base>-fn")
      - EB Rule -> SQS
    """

    sqs_queue: aws_sqs.Queue
    lambda_function: aws_lambda.Function

    def __init__(
        self, scope: Construct, construct_id: str, *, bus: aws_events.IEventBus, props: WorkerProps
    ) -> None:
        super().__init__(scope, construct_id)

        # --- naming ---
        queue_name = f"{props.base_name}-queue"
        dlq_name = f"{props.base_name}-dlq"
        function_name = f"{props.base_name}-fn"

        # --- resources ---
        dead_letter_queue = None
        if props.enable_dlq:
            dlq = aws_sqs.Queue(
                self,
                "DLQ",
                queue_name=dlq_name,
                retention_period=Duration.days(props.retention_days),
            )
            dead_letter_queue = aws_sqs.DeadLetterQueue(queue=dlq, max_receive_count=5)

        queue = aws_sqs.Queue(
            self,
            "Queue",
            queue_name=queue_name,
            visibility_timeout=Duration.seconds(props.visibility_timeout_sec),
            retention_period=Duration.days(props.retention_days),
            dead_letter_queue=dead_letter_queue,
        )

        log_group = aws_logs.LogGroup(
            self,
            "FnLogGroup",
            log_group_name=f"/aws/lambda/{function_name}",
            retention=props.log_retention_days,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fn = aws_lambda.Function(
            self,
            "Fn",
            function_name=function_name,
            runtime=props.runtime,
            handler=props.handler,
            code=aws_lambda.Code.from_asset(props.code_asset_path),
            memory_size=props.memory_mb,
            timeout=Duration.seconds(props.lambda_timeout_sec),
            environment={"QUEUE_URL": queue.queue_url, **(props.environment or {})},
            log_group=log_group,
        )

        fn.add_event_source(aws_lambda_event_sources.SqsEventSource(queue, batch_size=1))

        aws_events.Rule(
            self,
            "Rule",
            event_bus=bus,
            event_pattern=aws_events.EventPattern(
                source=[props.source],
                detail_type=[props.detail_type],
                detail={
                    "eventName": ["INSERT"],
                    "dynamodb": {"NewImage": {"eventName": {"S": props.match_event_names}}},
                },
            ),
            targets=[aws_events_targets.SqsQueue(queue)],  # type: ignore
        )

        self.sqs_queue = queue
        self.lambda_function = fn
