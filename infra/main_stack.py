from typing import Any

import aws_cdk as cdk
from aws_cdk import Stack
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

        # Start Job Worker
        start_job_worker_name = f"{stack_id}-start-job-worker"
        start_job_worker = WorkerConstruct(
            self,
            start_job_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=start_job_worker_name,
                match_event_names=["JOB_CREATED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/start_job_worker.zip",
                handler="start_job_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(start_job_worker.lambda_function)

        # Process Step Worker
        process_step_worker_name = f"{stack_id}-process-step-worker"
        process_step_worker = WorkerConstruct(
            self,
            process_step_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=process_step_worker_name,
                match_event_names=["JOB_STARTED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/process_step_worker.zip",
                handler="process_step_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(process_step_worker.lambda_function)

        # Complete Job Worker
        complete_job_worker_name = f"{stack_id}-complete-job-worker"
        complete_job_worker = WorkerConstruct(
            self,
            complete_job_worker_name,
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=complete_job_worker_name,
                match_event_names=["STEP_PROCESSED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/complete_job_worker.zip",
                handler="complete_job_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(complete_job_worker.lambda_function)

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
