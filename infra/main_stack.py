from typing import Any

import aws_cdk as cdk
from aws_cdk import Stack, aws_events, aws_events_targets
from constructs import Construct
from custom_constructs.api_construct import ApiConstruct
from custom_constructs.api_endpoint_construct import ApiEndpointConstruct, ApiEndpointProps
from custom_constructs.dynamodb_construct import DynamoDbConstruct
from custom_constructs.event_driver_construct import EventDriverConstruct
from custom_constructs.worker_construct import WorkerConstruct, WorkerProps


class MainStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        stack_id = construct_id

        #
        #
        #
        # ========================================================================
        # Build event driver components
        # ========================================================================

        # DynamoDB Table
        ddb_construct_name = f"{stack_id}-ddb"
        ddb_construct = DynamoDbConstruct(self, "DynamoDb", base_name=ddb_construct_name)

        # Event driver (DynamoDB Stream -> EventBridge Bus via Pipe)
        event_driver_name = f"{stack_id}-event-driver"
        event_driver = EventDriverConstruct(
            self,
            "EventDriver",
            base_name=event_driver_name,
            table_stream_arn=str(ddb_construct.table_stream_arn),
            source="app.inventory",
            detail_type="DynamoDBItemChange",
        )

        #
        #
        #
        # ========================================================================
        # Build workers
        # ========================================================================

        # Process Step Worker
        process_step_worker_name = f"{stack_id}-process-step-worker"
        process_step_worker = WorkerConstruct(
            self,
            process_step_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=process_step_worker_name,
                match_event_names=["JOB_CREATED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/process_step_worker.zip",
                handler="process_step_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(process_step_worker.lambda_function)

        # Execute Task Foo Worker
        execute_task_foo_worker_name = f"{stack_id}-execute-task-foo-worker"
        execute_task_foo_worker = WorkerConstruct(
            self,
            execute_task_foo_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=execute_task_foo_worker_name,
                match_event_names=["STEP_PROCESSED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/execute_task_foo_worker.zip",
                handler="execute_task_foo_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(execute_task_foo_worker.lambda_function)

        # Execute Task Qux Worker
        execute_task_qux_worker_name = f"{stack_id}-execute-task-qux-worker"
        execute_task_qux_worker = WorkerConstruct(
            self,
            execute_task_qux_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=execute_task_qux_worker_name,
                match_event_names=["STEP_PROCESSED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/execute_task_qux_worker.zip",
                handler="execute_task_qux_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(execute_task_qux_worker.lambda_function)

        # Execute Task Bar Worker
        execute_task_bar_worker_name = f"{stack_id}-execute-task-bar-worker"
        execute_task_bar_worker = WorkerConstruct(
            self,
            execute_task_bar_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=execute_task_bar_worker_name,
                match_event_names=["STEP_PROCESSED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/execute_task_bar_worker.zip",
                handler="execute_task_bar_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(execute_task_bar_worker.lambda_function)

        # Complete All Tasks Worker
        # This worker listens to all three task events, so we need three EventBridge Rules
        complete_all_tasks_worker_name = f"{stack_id}-complete-all-tasks-worker"
        complete_all_tasks_worker = WorkerConstruct(
            self,
            complete_all_tasks_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=complete_all_tasks_worker_name,
                match_event_names=["TASK_FOO_EXECUTED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/complete_all_tasks_worker.zip",
                handler="complete_all_tasks_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(complete_all_tasks_worker.lambda_function)

        # Add additional EventBridge Rules for TASK_QUX_EXECUTED_EVENT and TASK_BAR_EXECUTED_EVENT
        # Both route to the same SQS queue
        aws_events.Rule(
            self,
            f"{complete_all_tasks_worker_name}-rule-qux",
            event_bus=event_driver.bus,
            event_pattern=aws_events.EventPattern(
                source=["app.inventory"],
                detail_type=["DynamoDBItemChange"],
                detail={
                    "eventName": ["INSERT"],
                    "dynamodb": {"NewImage": {"eventName": {"S": ["TASK_QUX_EXECUTED_EVENT"]}}},
                },
            ),
            targets=[aws_events_targets.SqsQueue(complete_all_tasks_worker.sqs_queue)],  # type: ignore
        )

        aws_events.Rule(
            self,
            f"{complete_all_tasks_worker_name}-rule-bar",
            event_bus=event_driver.bus,
            event_pattern=aws_events.EventPattern(
                source=["app.inventory"],
                detail_type=["DynamoDBItemChange"],
                detail={
                    "eventName": ["INSERT"],
                    "dynamodb": {"NewImage": {"eventName": {"S": ["TASK_BAR_EXECUTED_EVENT"]}}},
                },
            ),
            targets=[aws_events_targets.SqsQueue(complete_all_tasks_worker.sqs_queue)],  # type: ignore
        )

        # Finalize Job Worker
        finalize_job_worker_name = f"{stack_id}-finalize-job-worker"
        finalize_job_worker = WorkerConstruct(
            self,
            finalize_job_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=finalize_job_worker_name,
                match_event_names=["ALL_TASKS_COMPLETED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/finalize_job_worker.zip",
                handler="finalize_job_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(finalize_job_worker.lambda_function)

        #
        #
        #
        # ========================================================================
        # Build api & endpoints
        # ========================================================================

        # 4) HTTP API
        api_construct_name = f"{stack_id}-apigw"
        api = ApiConstruct(self, api_construct_name, base_name=api_construct_name)

        # Create Job Endpoint
        # POST /jobs
        create_jobs_endpoint_name = f"{stack_id}-create-job-endpoint"
        create_jobs_endpoint = ApiEndpointConstruct(
            self,
            create_jobs_endpoint_name,
            http_api=api.http_api,
            props=ApiEndpointProps(
                base_name=create_jobs_endpoint_name,
                method="POST",
                path="/jobs",
                code_asset_path=".dist/services/create_job_endpoint.zip",
                handler="create_job_endpoint.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(create_jobs_endpoint.lambda_function)

        # List Job Events Endpoint
        # GET /jobs/{job_id}/events
        list_job_events_endpoint_name = f"{stack_id}-list-job-events-endpoint"
        list_job_events_endpoint = ApiEndpointConstruct(
            self,
            list_job_events_endpoint_name,
            http_api=api.http_api,
            props=ApiEndpointProps(
                base_name=list_job_events_endpoint_name,
                method="GET",
                path="/jobs/{job_id}/events",
                code_asset_path=".dist/services/list_job_events_endpoint.zip",
                handler="list_job_events_endpoint.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(list_job_events_endpoint.lambda_function)

        #
        #
        #
        # ========================================================================
        # Outputs
        # ========================================================================

        cdk.CfnOutput(self, "TableName", value=ddb_construct.table.table_name)
        cdk.CfnOutput(self, "EventBusName", value=event_driver.bus.event_bus_name)
        cdk.CfnOutput(self, "ApiEndpoint", value=api.http_api.api_endpoint)
