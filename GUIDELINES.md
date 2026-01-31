# Development Guidelines: Python DynamoDB Event-Driven Template

This comprehensive guide provides everything needed to understand, develop, and maintain the Python DynamoDB Event-Driven Template project. This document consolidates all architectural knowledge, development patterns, testing requirements, and infrastructure guidelines into a single reference.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure](#project-structure)
4. [Understanding Workers and APIs](#understanding-workers-and-apis)
5. [Event-Driven Architecture Approach](#event-driven-architecture-approach)
6. [Infrastructure and How It Ties Together](#infrastructure-and-how-it-ties-together)
7. [Key Design Patterns and Principles](#key-design-patterns-and-principles)
8. [Complete Event Flow Example](#complete-event-flow-example)
9. [Adding a New Feature](#adding-a-new-feature)
10. [Creating Domain Events](#creating-domain-events)
11. [Implementing APIs](#implementing-apis)
12. [Implementing Workers](#implementing-workers)
13. [Error Handling](#error-handling)
14. [Testing Guidelines](#testing-guidelines)
15. [Infrastructure Guidelines](#infrastructure-guidelines)
16. [Best Practices](#best-practices)
17. [Common Pitfalls](#common-pitfalls)
18. [Quick Reference](#quick-reference)

---

## Executive Summary

This project is a **Python template for building event-driven applications on AWS** using:

- **Event Sourcing** pattern with DynamoDB as the event store
- **EventBridge** for event routing
- **SQS + Lambda** for asynchronous worker processing
- **API Gateway + Lambda** for synchronous API endpoints
- **Strict Type Safety** with Python type hints and Pydantic validation

### Core Components

**Event Store System:**

- `EventBase` - Base Pydantic model for domain events with `from_dynamodb_record()` and `from_eventbridge_sqs_record()` methods
- `EventStoreClient` - Publishes events to DynamoDB with idempotency checks and queries event history
  - `raise_event()` - Saves event to DynamoDB with idempotency check
  - `list_events_for_job()` - Queries all events for a specific job, returns sorted list
- Event classes - Pydantic models extending `EventBase` (e.g., `JobCreatedEvent`, `StepProcessedEvent`, `TaskFooExecutedEvent`, `TaskQuxExecutedEvent`, `TaskBarExecutedEvent`, `AllTasksCompletedEvent`, `JobFinalizedEvent`)

**Error Handling System:**

- `ErrorBase` - Base exception class with transient flag
- `ErrorTransient` - For retryable errors (network issues, throttling)
- `ErrorPermanent` - For non-retryable errors (validation failures)
- `ErrorEventAlreadyRaisedException` - For duplicate events (idempotency)
- `ErrorInvalidArgumentsException` - For invalid input validation

**Common Infrastructure:**

- `DynamoDbConstruct` - Event store table with streams enabled
- `EventDriverConstruct` - EventBridge setup with DynamoDB pipe

---

## Architecture Overview

### Event-Driven Architecture Flow

```
        ┌─────────────┐
        │ REST Client │
        └──────┬──────┘
               │ HTTP POST
               ▼
┌──────────────────────────────────────────────────────────┐
│                      API LAYER                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ API Gateway v2 → CreateJobEndpoint Lambda Handler  │  │
│  │   └─> Validates input                              │  │
│  │       └─> EventStoreClient.raise_event()           │  │
│  └────────────────────────────────────────────────────┘  │
└───────────────────────────┬──────────────────────────────┘
                            │
                            │ Store Event
                            ▼
┌──────────────────────────────────────────────────────────┐
│                EVENT STORE (DynamoDB)                    │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Table: Events                                      │  │
│  │ - pk: EVENTS#JOB_ID#ABC-123                        │  │
│  │ - sk: EVENT#JOB_CREATED_EVENT                      │  │
│  │ - eventName, eventData, createdAt, idempotencyKey  │  │
│  │ - Stream: NEW_IMAGE enabled                        │  │
│  └────────────────────────────────────────────────────┘  │
└───────────────────────────┬──────────────────────────────┘
                            │
                            │ DynamoDB Stream
                            ▼
┌──────────────────────────────────────────────────────────┐
│              EVENT ROUTING (EventBridge)                 │
│  ┌────────────────────────────────────────────────────┐  │
│  │ EventBridge Pipe: DynamoDB Stream → EventBus       │  │
│  │ EventBridge Rule: Filter by eventName              │  │
│  │   └─> Route to SQS Queue                           │  │
│  └────────────────────────────────────────────────────┘  │
└───────────────────────────┬──────────────────────────────┘
                            │
                            │ SQS Message
                            ▼
┌──────────────────────────────────────────────────────────┐
│                    WORKER LAYER                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ SQS Queue → StartJobWorker Lambda Handler          │  │
│  │   └─> EventBase.from_eventbridge_sqs_record()      │  │
│  │       └─> EventStoreClient.raise_event()           │  │
│  │           (creates JOB_STARTED_EVENT)              │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Key Architectural Patterns

1. **Event Sourcing**: All state changes are stored as immutable events
2. **CQRS-like separation**: APIs write events, Workers process events
3. **Event-Driven**: Components communicate via events, not direct calls
4. **Idempotency**: Events use idempotency keys to prevent duplicates
5. **Exception-Based Error Handling**: Python exceptions with transient flags for retry logic

### Event Flow Example

The system supports complex event chains with parallel task execution. For example:

```
JOB_CREATED_EVENT → STEP_PROCESSED_EVENT → TASK_FOO_EXECUTED_EVENT ──┐
                                                                      ├──→ ALL_TASKS_COMPLETED_EVENT → JOB_FINALIZED_EVENT
                                                      TASK_QUX_EXECUTED_EVENT ──┼
                                                      TASK_BAR_EXECUTED_EVENT ──┘
```

---

## Project Structure

### Root Level

```
python-ddb-event-driven/
├── services/          # Lambda function code (business logic)
├── infra/             # AWS CDK infrastructure code
├── _restclient/       # HTTP test files for VSCode REST Client
├── deploy-scripts/    # Build and deployment automation
└── README.md          # Project documentation
```

### Services Directory (`services/`)

```
services/
├── __events/                           # Core event system (shared)
│   ├── __init__.py
│   ├── event_base.py                   # Base class for all events
│   ├── event_store_client.py           # Publishes events to DynamoDB
│   ├── job_created_event.py            # Job creation event
│   ├── step_processed_event.py         # Step processed event
│   ├── task_foo_executed_event.py      # Task Foo executed event
│   ├── task_qux_executed_event.py     # Task Qux executed event
│   ├── task_bar_executed_event.py     # Task Bar executed event
│   ├── all_tasks_completed_event.py    # All tasks completed event
│   └── job_finalized_event.py          # Job finalized event
│
├── __errors/                           # Error handling (shared)
│   ├── __init__.py
│   ├── error_base.py                   # Base exception class
│   ├── error_transient.py              # Transient error exception
│   ├── error_permanent.py              # Permanent error exception
│   ├── error_event_already_raised.py   # Duplicate event exception
│   └── error_invalid_arguments.py      # Invalid arguments exception
│
├── __http_helpers/                     # Shared utilities
│   ├── __init__.py
│   └── http_response.py                # HTTP response helpers
│
├── create_job_endpoint/                # API endpoint implementation
│   ├── create_job_endpoint.py          # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── list_job_events_endpoint/           # API endpoint implementation
│   ├── list_job_events_endpoint.py     # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── process_step_worker/                # Worker implementation
│   ├── process_step_worker.py          # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── execute_task_foo_worker/            # Worker implementation
│   ├── execute_task_foo_worker.py      # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── execute_task_qux_worker/            # Worker implementation
│   ├── execute_task_qux_worker.py      # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── execute_task_bar_worker/            # Worker implementation
│   ├── execute_task_bar_worker.py      # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
├── complete_all_tasks_worker/          # Worker implementation
│   ├── complete_all_tasks_worker.py     # Lambda handler
│   └── requirements.txt                # Lambda dependencies
│
└── finalize_job_worker/                # Worker implementation
    ├── finalize_job_worker.py          # Lambda handler
    └── requirements.txt                # Lambda dependencies
```

### Infrastructure Directory (`infra/`)

```
infra/
├── app.py                              # CDK app entry point
├── main_stack.py                       # Root CDK stack
└── custom_constructs/                  # Reusable CDK constructs
    ├── __init__.py
    ├── api_construct.py                # HTTP API Gateway setup
    ├── api_endpoint_construct.py       # API endpoint + Lambda
    ├── dynamodb_construct.py           # DynamoDB table with streams
    ├── event_driver_construct.py       # Stream → EventBridge pipe
    └── worker_construct.py             # Worker Lambda + SQS + EventBridge
```

### Build System Conventions

**Deployable Lambda Detection:**

A folder inside `services/` is considered a **"deployable Lambda"** if it contains a Python file that matches the folder's name.

**Examples:**

- `services/create_job_endpoint/create_job_endpoint.py` ✅ (deployable)
- `services/process_step_worker/process_step_worker.py` ✅ (deployable)
- `services/__events/event_base.py` ❌ (shared module, not deployable)

**Shared Module Convention:**

Any folder inside `services/` that starts with a **double underscore** (`__`) is considered a **"shared module"**.

**Shared modules:**

- `__events/` - Event classes and parsing logic
- `__errors/` - Custom exception hierarchy
- `__http_helpers/` - HTTP response utilities

**Build Process:**

1. **Build command**: `pipenv run build-dist` (packages all Lambdas into ZIP files)
2. **Output location**: `.dist/services/{service_name}.zip`
3. **Infrastructure requirement**: Code asset paths must point to ZIP files (e.g., `".dist/services/create_job_endpoint.zip"`)
4. **Build script**: `deploy-scripts/_build_helpers.sh` handles packaging

**CRITICAL**: Always run `pipenv run build-dist` before deploying infrastructure. Infrastructure code references ZIP files, not source directories.

---

## Understanding Workers and APIs

### APIs (Synchronous Request-Response)

**Purpose**: Handle HTTP requests from clients, validate input, and publish events.

**Flow**:

1. Client sends HTTP request → API Gateway
2. API Gateway routes to Lambda handler
3. Handler validates input using Pydantic models
4. Handler publishes event to DynamoDB via EventStoreClient
5. Returns HTTP response (202 Accepted typically)

**Example: CreateJobEndpoint**

```python
# Handler (entry point)
create_job_endpoint.py
  └─> Validate IncomingCreateJobRequest (Pydantic)
      └─> Build JobCreatedEvent
          └─> EventStoreClient.raise_event()
```

**Characteristics**:

- Fast response times (typically < 1 second)
- Returns immediately after event is stored
- Uses API Gateway v2 (HTTP API)
- Timeout: 29 seconds (API Gateway limit)

### Workers (Asynchronous Event Processing)

**Purpose**: Process events from the event store, perform business logic, and optionally publish new events.

**Flow**:

1. Event stored in DynamoDB → Triggers DynamoDB Stream
2. EventBridge Pipe forwards to EventBridge Bus
3. EventBridge Rule filters and routes to SQS Queue
4. Lambda polls SQS, processes batch of messages
5. Worker reconstitutes event, processes it, may publish new events

**Example: ProcessStepWorker**

```python
# Handler (entry point)
process_step_worker.py
  └─> Iterate SQS records
      └─> EventBase.from_eventbridge_sqs_record()  # Reconstitute event
          └─> Process business logic
              └─> EventStoreClient.raise_event(StepProcessedEvent)
```

**Characteristics**:

- Asynchronous processing (can take longer)
- Batch processing from SQS (up to 10 messages)
- Dead Letter Queue for failed messages
- Retry logic via SQS visibility timeout (only for transient errors)
- Timeout: 60 seconds (configurable)

---

## Event-Driven Architecture Approach

### Event Sourcing Pattern

**Core Principle**: Store all state changes as a sequence of immutable events.

**Benefits**:

- Complete audit trail
- Time travel (replay events to any point in time)
- Decoupled components (no direct dependencies)
- Scalability (events can be processed independently)

### Event Lifecycle

1. **Event Creation**:

   ```python
   event = JobCreatedEvent.from_data(
       job_id=job_id,
       job_name=job_name,
       job_status="CREATED",
   )
   # from_data() automatically generates:
   # - idempotencyKey: f"JOB_ID#{job_id}"
   # - createdAt: datetime.now().isoformat()
   ```

2. **Event Storage**:

   ```python
   event_store_client.raise_event(event)
   # Stores in DynamoDB with idempotency check
   ```

3. **Event Streaming**:
   - DynamoDB Stream captures NEW_IMAGE
   - EventBridge Pipe forwards to EventBridge Bus

4. **Event Routing**:
   - EventBridge Rule filters by `eventName`
   - Routes matching events to SQS Queue

5. **Event Processing**:

   ```python
   incoming_event = EventBase.from_eventbridge_sqs_record(
       sqs_record, JobCreatedEvent
   )
   # Reconstitutes event from EventBridge payload
   ```

   **Event Reconstitution Methods:**

   - **`EventBase.from_eventbridge_sqs_record()`**: Use in workers to reconstitute events from SQS records
     - Input: SQS record with EventBridge event payload in body
     - Flow: SQS Record → Body (JSON string) → EventBridge event → Detail (DynamoDB record) → NewImage
     - **Always use this in workers** (workers receive events via SQS)

   - **`EventBase.from_dynamodb_record()`**: Use when processing DynamoDB Stream records directly
     - Input: Raw DynamoDB Stream record
     - Flow: DynamoDB Stream record → NewImage → Deserialize → Event
     - **Rarely used** (EventBridge Pipe handles stream transformation)

   **When to use which:**
   - ✅ **Workers**: Always use `from_eventbridge_sqs_record()` (events come via SQS)
   - ❌ **Workers**: Never use `from_dynamodb_record()` (EventBridge Pipe transforms streams)
   - ✅ **Direct stream processing**: Use `from_dynamodb_record()` (if bypassing EventBridge)

6. **Event Continuation**:
   - Worker processes event
   - May publish new events (creating event chains)

7. **Event Querying**:

   ```python
   events = event_store_client.list_events_for_job(job_id)
   # Returns list of EventBase objects sorted by createdAt
   # Queries DynamoDB for all events with pk = "EVENTS#JOB_ID#{job_id}"
   ```

### Event Structure

```python
class EventBase(BaseModel):
    idempotencyKey: str  # Prevents duplicates
    eventName: str  # e.g., "JOB_CREATED_EVENT"
    eventData: Any  # Domain-specific Pydantic model
    createdAt: str  # ISO timestamp
```

### DynamoDB Key Structure

Events are stored in DynamoDB with a composite key structure:

- **Partition Key (pk)**: `EVENTS#{idempotencyKey}`
  - Example: `EVENTS#JOB_ID#JOB-123`
  - Used for querying all events for a job: `EVENTS#JOB_ID#{job_id}`
- **Sort Key (sk)**: `EVENT#{eventName}`
  - Example: `EVENT#JOB_CREATED_EVENT`
  - Ensures uniqueness per event type within a job

**Design Rationale:**

- **Composite key (pk + sk)**: Enables efficient querying of all events for a job while ensuring uniqueness per event type
- **Query pattern**: `pk = "EVENTS#JOB_ID#{job_id}" AND begins_with(sk, "EVENT#")` retrieves all events for a job
- **Uniqueness**: The combination of `pk` and `sk` ensures no duplicate events (idempotency check uses `attribute_not_exists(sk)`)

**Storage Example:**

```
pk: EVENTS#JOB_ID#JOB-123
sk: EVENT#JOB_CREATED_EVENT
eventName: JOB_CREATED_EVENT
eventData: { "job_id": "JOB-123", "job_name": "Sample Job", "job_status": "CREATED" }
createdAt: "2024-01-01T12:00:00Z"
idempotencyKey: JOB_ID#JOB-123
```

### Idempotency

Events use **idempotency keys** to prevent duplicate processing:

**Idempotency Key Format Guidelines:**

1. **Standard Format**: `{ENTITY_TYPE}#{ENTITY_ID}`
   - Example: `JOB_ID#JOB-123`
   - Use for events that represent a single state change per entity

2. **Custom Format**: When events need more granular uniqueness
   - Example: `JOB_ID#JOB-123:task:foo` (if multiple task events per job)
   - Only deviate from standard format when necessary

3. **Determining Format**:
   - **Question**: "Can this event happen multiple times for the same entity?"
   - **If NO**: Use standard format `{ENTITY_TYPE}#{ENTITY_ID}`
   - **If YES**: Add additional qualifiers to ensure uniqueness

4. **Implementation in `from_data()`**:
   ```python
   @classmethod
   def from_data(cls, job_id: str, job_name: str, job_status: str) -> JobCreatedEvent:
       # Standard format: JOB_ID#{job_id}
       idempotencyKey = f"JOB_ID#{job_id}"
       # ... rest of implementation
   ```

**DynamoDB Idempotency Check:**

- Condition: `attribute_not_exists(sk)` (checks sort key, not partition key)
- Rationale: Multiple events can share the same `pk` (same job), but each event type must be unique per job
- Duplicate events raise `ErrorEventAlreadyRaisedException` (non-transient)

---

## Infrastructure and How It Ties Together

### Infrastructure Hierarchy

```
MainStack (Root)
├── Common Infrastructure
│   ├── DynamoDbConstruct
│   │   └── Table with streams, GSI
│   └── EventDriverConstruct
│       └── EventBus + EventBridge Pipe (DynamoDB → EventBus)
│
└── Service Infrastructure
    ├── ApiConstruct
    │   └── HTTP API (API Gateway v2)
    │
    ├── CreateJobEndpointConstruct
    │   ├── Lambda Function
    │   ├── API Gateway Route
    │   └── DynamoDB Permissions
    │
    └── StartJobWorkerConstruct
        ├── SQS Queue + DLQ
        ├── Lambda Function (SQS trigger)
        ├── EventBridge Rule (filters events)
        └── DynamoDB Permissions
```

### How Infrastructure Ties to Services

#### 1. **API Infrastructure → API Handler**

```python
# infra/custom_constructs/api_endpoint_construct.py
NodejsFunction(
    entry="services/create_job_endpoint/create_job_endpoint.py",
    handler="handler",
    environment={
        "TABLE_NAME": dynamoDbTable.table_name,
    },
)

# infra/custom_constructs/api_construct.py
http_api.add_routes(
    path="/jobs",
    methods=[HttpMethod.POST],
    integration=lambda_integration,
)
```

**Connection**: API Gateway route → Lambda handler → EventStoreClient

#### 2. **Worker Infrastructure → Worker Handler**

```python
# infra/main_stack.py
worker = WorkerConstruct(
    self,
    "ProcessStepWorker",
    bus=event_driver.bus,
    props=WorkerProps(
        base_name=f"{stack_id}-process-step-worker",
        match_event_names=["JOB_CREATED_EVENT"],
        source="app.inventory",
        detail_type="DynamoDBItemChange",
        code_asset_path=".dist/services/process_step_worker.zip",
        handler="process_step_worker.handler",
        environment={"TABLE_NAME": ddb_construct.table.table_name},
    ),
)
ddb_construct.table.grant_read_write_data(worker.lambda_function)
```

**Connection**: DynamoDB Stream → EventBridge Pipe → EventBridge Bus → EventBridge Rule → SQS Queue → Lambda handler → EventStoreClient

**What WorkerConstruct creates internally:**
- SQS Queue with DLQ
- Lambda Function with SQS event source (batch_size=1)
- EventBridge Rule filtering by `eventName` and routing to SQS
- CloudWatch Log Group

#### 3. **Event Store Infrastructure**

```python
# infra/custom_constructs/dynamodb_construct.py
Table(
    partition_key=Attribute(name="pk", type=AttributeType.STRING),
    sort_key=Attribute(name="sk", type=AttributeType.STRING),
    stream=StreamViewType.NEW_IMAGE,  # Enables DynamoDB Streams
)

# infra/custom_constructs/event_driver_construct.py
CfnPipe(
    source=dynamoDbTable.table_stream_arn,
    target=eventBus.event_bus_arn,
    # Transforms DynamoDB stream records to EventBridge events
)
```

**Connection**: EventStoreClient writes to DynamoDB → DynamoDB Stream → EventBridge Pipe → EventBridge Bus

### Environment Variables

All Lambda functions receive:

- `TABLE_NAME`: DynamoDB table name for event storage

### Permissions (IAM)

- **API Lambdas**: `dynamoDbTable.grant_read_write_data(lambda_func)`
- **Worker Lambdas**: `dynamoDbTable.grant_read_write_data(lambda_func)` + `queue.grant_consume_messages(lambda_func)`
- **EventBridge Pipe Role**: `dynamoDbTable.grant_stream_read(role)` + `eventBus.grant_put_events_to(role)`

---

## Key Design Patterns and Principles

### 1. **Exception-Based Error Handling**

Python uses exceptions instead of Result types:

```python
# ✅ Good
try:
    event_store_client.raise_event(event)
except ErrorEventAlreadyRaisedException:
    # Handle duplicate event (non-transient)
    pass
except ErrorTransient as e:
    # Handle transient error (retry)
    raise
except ErrorPermanent as e:
    # Handle permanent error (don't retry)
    pass
```

**Benefits**:

- Native Python pattern
- Explicit error handling
- Transient vs non-transient failures
- Type-safe error types

### 2. **Pydantic Validation**

All inputs validated using Pydantic models:

```python
class IncomingCreateJobRequest(BaseModel):
    job_id: str
    job_name: str

# Automatic validation
request = IncomingCreateJobRequest.model_validate_json(body_str)
```

### 3. **Type Hints and Strict Typing**

All functions use explicit type hints with strict type checking:

```python
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from aws_lambda_typing.events import APIGatewayProxyEventV2
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.responses import APIGatewayProxyResponseV2
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    Context = Any
    APIGatewayProxyEventV2 = Dict[str, Any]
    APIGatewayProxyResponseV2 = Dict[str, Any]
    DynamoDBServiceResource = Any

def handler(event: APIGatewayProxyEventV2, context: Context) -> APIGatewayProxyResponseV2:
    ...
```

**TYPE_CHECKING Pattern Explained:**

- **Purpose**: Import type-only dependencies without runtime overhead
- **How it works**: `TYPE_CHECKING` is `False` at runtime, `True` during static type checking
- **Runtime**: Uses `Dict[str, Any]` (compatible with actual Lambda event structure)
- **Type checking**: Uses proper types from `aws_lambda_typing` for accurate type hints
- **Benefits**:
  - Type safety with mypy/pyright
  - Better IDE support (autocomplete, type checking)
  - No runtime dependency on type-only packages
  - Compatible with actual AWS Lambda event structures

### 4. **Event Factory Pattern**

Events use factory methods for controlled creation:

- **`from_data()` class method**: Creates new event from domain data (used in APIs and workers)
  - Auto-generates `idempotencyKey` and `createdAt`
  - Validates input using Pydantic
  - **Always use this for manual event creation**
- **`from_eventbridge_sqs_record()` static method**: Rebuilds event from SQS/EventBridge payload (used in workers)
  - Parses EventBridge event structure
  - **Always use this in workers to reconstitute events**
- **`from_dynamodb_record()` static method**: Rebuilds event from DynamoDB stream record (rarely used)
  - Parses raw DynamoDB Stream record
  - **Only use if bypassing EventBridge Pipe**

### 5. **Consolidated Execution Pattern**

For clients making external calls (DynamoDB, S3, external APIs), consolidate request building and execution into a single method with appropriate try-catch blocks to isolate different operations and errors.

The number of try-catch blocks depends on the distinct operations that can fail independently. For example, in `EventStoreClient.raise_event()`, we use try-catch blocks to isolate:

1. **Command execution** (can fail due to network errors, DynamoDB errors, or conditional check failures)
2. **Error classification** (transient vs permanent based on error codes)

**Benefits**:

- Reduces indirection
- Maintains error handling granularity
- Clearer intent
- Better cohesion

---

## Complete Event Flow Example

### Scenario: Create a Job

1. **Client Request**:

   ```http
   POST /jobs
   { "job_id": "JOB-123", "job_name": "Sample Job" }
   ```

2. **API Handler** (`create_job_endpoint.py`):
   - Validates input using `IncomingCreateJobRequest.model_validate_json()`
   - Builds `JobCreatedEvent` using `JobCreatedEvent.from_data()` factory method
   - Calls `event_store_client.raise_event(event)`

3. **EventStoreClient**:
   - Validates event
   - Executes DynamoDB `put_item()` with condition `attribute_not_exists(pk)`
   - Uses idempotency check to prevent duplicates
   - Stores in DynamoDB:
     ```
     pk: EVENTS#JOB_ID#JOB-123
     sk: EVENT#JOB_CREATED_EVENT
     eventName: JOB_CREATED_EVENT
     eventData: { job_id: "JOB-123", job_name: "Sample Job", job_status: "CREATED" }
     createdAt: "2024-01-01T12:00:00Z"
     ```

4. **DynamoDB Stream**:
   - Captures NEW_IMAGE record
   - Forwards to EventBridge Pipe

5. **EventBridge Pipe**:
   - Transforms DynamoDB stream record to EventBridge event
   - Publishes to EventBridge Bus

6. **EventBridge Rule**:
   - Matches events where `eventName = "JOB_CREATED_EVENT"`
   - Routes to SQS Queue

7. **SQS Queue**:
   - Receives message with EventBridge event payload
   - Lambda polls queue (batch of up to 10 messages)

8. **Worker Handler** (`process_step_worker.py`):
   - Iterates SQS records
   - Calls `EventBase.from_eventbridge_sqs_record(sqs_record, JobCreatedEvent)` to reconstitute event
   - Processes the step (business logic)
   - Builds `StepProcessedEvent` using `StepProcessedEvent.from_data()` factory method
   - Calls `event_store_client.raise_event(new_event)`

9. **Event Chain Continues**:
   - `STEP_PROCESSED_EVENT` stored in DynamoDB
   - Triggers DynamoDB Stream event
   - Three workers process in parallel:
     - `ExecuteTaskFooWorker` → `TASK_FOO_EXECUTED_EVENT`
     - `ExecuteTaskQuxWorker` → `TASK_QUX_EXECUTED_EVENT`
     - `ExecuteTaskBarWorker` → `TASK_BAR_EXECUTED_EVENT`
   - `CompleteAllTasksWorker` waits for all three task events → `ALL_TASKS_COMPLETED_EVENT`
   - `FinalizeJobWorker` processes completion → `JOB_FINALIZED_EVENT`

10. **Event Querying** (via API):
    - Client can query all events for a job using `GET /jobs/{jobId}/events`
    - API handler calls `event_store_client.list_events_for_job(job_id)`
    - Returns all events for the job sorted by `createdAt`

> **Note**: For a complete visual representation of the event flow, see the [Event Flow Example](README.md#event-flow-example) section in README.md.

---

## Adding a New Feature

When adding a new feature (e.g., "Cancel Job"), you typically need:

1. **Domain Event** (if creating new state)
2. **API Endpoint** (if exposing HTTP endpoint)
3. **Worker** (if processing events asynchronously)
4. **Infrastructure** (Lambda, routes, rules, queues)
5. **Tests** (MANDATORY - see below)

### ⚠️ CRITICAL: Testing Requirement

**MANDATORY**: You MUST create comprehensive test files for ALL components when developing a new feature. This is not optional.

**For every component created, you must create a corresponding test file:**

- ✅ Every Event class → `{event_name}_test.py`
- ✅ Every Handler file → `{handler_name}_test.py`
- ✅ Every Client class → `{client_name}_test.py` (if applicable)

**Test files must:**

- Be co-located with the component being tested or in a `tests/` subdirectory
- Follow the naming pattern `{component_name}_test.py`
- Follow the testing patterns documented in the [Testing Guidelines](#testing-guidelines) section
- Cover edge cases, internal logic, and expected results

**DO NOT** mark a feature as complete without creating all corresponding test files.

### Feature Implementation Checklist

When adding a new feature, follow these checklists in order. See [Quick Reference Checklists](#checklists) section below for detailed item-by-item checklists.

**High-Level Steps:**

1. **Event (if needed)**
   - [ ] Create event class following [Event Creation Checklist](#event-creation-checklist)
   - [ ] Add event name constant

2. **API Endpoint (if needed)**
   - [ ] Create API endpoint following [API Implementation Checklist](#api-implementation-checklist)
   - [ ] Create infrastructure following [API Infrastructure Checklist](#infrastructure-checklist)

3. **Worker (if needed)**
   - [ ] Create Worker following [Worker Implementation Checklist](#worker-implementation-checklist)
   - [ ] Create infrastructure following [Worker Infrastructure Checklist](#infrastructure-checklist)

4. **Final Steps**
   - [ ] Wire up infrastructure in main stack
   - [ ] Update documentation
   - [ ] Verify all tests pass

---

## Creating Domain Events

### Event Structure

All events must:

1. Extend `EventBase` (Pydantic BaseModel)
2. Define a unique `eventName` class attribute
3. Define `eventData` as a Pydantic model
4. Implement `from_data()` class method factory for event creation
5. Use Pydantic for validation

### Event Template

```python
from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel

from services.__events.event_base import EventBase


class JobCancelledEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str


class JobCancelledEvent(EventBase):
    eventName: str = "JOB_CANCELLED_EVENT"
    eventData: JobCancelledEventData

    @classmethod
    def from_data(cls, job_id: str, job_name: str, job_status: str) -> JobCancelledEvent:
        """
        Factory method to create a JobCancelledEvent from event data fields.
        Use this for manual event creation in APIs and workers.
        """
        event_data = JobCancelledEventData(
            job_id=job_id,
            job_name=job_name,
            job_status=job_status,
        )

        createdAt = datetime.now().isoformat()
        idempotencyKey = f"JOB_ID#{job_id}"

        return cls(
            idempotencyKey=idempotencyKey,
            createdAt=createdAt,
            eventData=event_data,
        )
```

### Event Creation

Events should be created using the `from_data()` factory method, which automatically generates `idempotencyKey` and `createdAt`:

```python
event = JobCancelledEvent.from_data(
    job_id=job_id,
    job_name=job_name,
    job_status="CANCELLED",
)
# from_data() automatically generates:
# - idempotencyKey: f"JOB_ID#{job_id}"
# - createdAt: datetime.now().isoformat()
```

> **Note**: The `from_data()` factory method ensures consistent event creation and prevents tampering with `idempotencyKey` and `createdAt`. For reconstituting events from storage (DynamoDB/EventBridge), use `EventBase.from_dynamodb_record()` or `EventBase.from_eventbridge_sqs_record()`.

### Event Naming Conventions

- **Event Names**: `UPPER_SNAKE_CASE` (e.g., `JOB_CANCELLED_EVENT`)
- **Event Classes**: `PascalCase` (e.g., `JobCancelledEvent`)
- **Event Data Classes**: `PascalCase` + `Data` (e.g., `JobCancelledEventData`)

### Idempotency Key Guidelines

- **Format**: `{entityType}#{entityId}` or custom format based on event uniqueness
- **Example**: `JOB_ID#JOB-123` or `JOB_ID#JOB-123:cancelled:true`
- **Purpose**: Prevents duplicate event processing
- **Uniqueness**: Must be unique per event instance

---

## Implementing APIs

### API Endpoint Structure

```
<endpoint_name>/
├── <endpoint_name>.py        # Lambda handler
└── requirements.txt          # Lambda dependencies
```

### Handler Guidelines

**Responsibilities**:

- Parse AWS API Gateway event
- Validate input using Pydantic models
- For write endpoints: Build domain event using `Event.from_data()` factory method and publish via `EventStoreClient.raise_event()`
- For query endpoints: Query events via `EventStoreClient.list_events_for_job()`
- Return HTTP response
- Handle error mapping (exceptions → HTTP status codes)

**Template**:

```python
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

import boto3
from pydantic import BaseModel, ValidationError

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__events.event_store_client import EventStoreClient
from services.__events.job_cancelled_event import JobCancelledEvent, JobCancelledEventData
from services.__http_helpers.http_response import HttpResponse

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import APIGatewayProxyEventV2
    from aws_lambda_typing.responses import APIGatewayProxyResponseV2
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    Context = Any
    APIGatewayProxyEventV2 = Dict[str, Any]
    APIGatewayProxyResponseV2 = Dict[str, Any]
    DynamoDBServiceResource = Any


class IncomingCancelJobRequest(BaseModel):
    job_id: str
    job_name: str


dynamodb_client: DynamoDBServiceResource = boto3.resource("dynamodb")  # type: ignore

TABLE_NAME = os.environ.get("TABLE_NAME")
if not TABLE_NAME:
    raise ValueError("'TABLE_NAME' environment variable not set.")

event_store_client = EventStoreClient(dynamodb_client, TABLE_NAME)


def handler(event: APIGatewayProxyEventV2, _context: Context) -> APIGatewayProxyResponseV2:
    """
    API Gateway Lambda handler to cancel a job.
    POST /jobs/{job_id}/cancel
    """
    body_str = event.get("body")
    if not body_str:
        print("ERROR: Request body is required")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": "Request body is required"}
        )

    try:
        incoming_request = IncomingCancelJobRequest.model_validate_json(body_str)
    except ValidationError as e:
        print(f"ERROR: Failed to validate IncomingCancelJobRequest: {e}")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": str(e)}
        )

    event = JobCancelledEvent.from_data(
        job_id=incoming_request.job_id,
        job_name=incoming_request.job_name,
        job_status="CANCELLED",
    )

    try:
        event_store_client.raise_event(event)
        print(f"SUCCESS: JobCancelledEvent raised for job ID: {event.idempotencyKey}")
        return HttpResponse.api_gateway_responseV2(
            202, {"message": "Accepted", "result": incoming_request.model_dump()}
        )

    except ErrorEventAlreadyRaisedException:
        print(f"INFO: Idempotent request received for job ID: {event.idempotencyKey}")
        return HttpResponse.api_gateway_responseV2(
            202, {"message": "Accepted", "result": incoming_request.model_dump()}
        )

    except Exception as e:
        print(f"ERROR: Failed to raise JobCancelledEvent: {e}")
        return HttpResponse.api_gateway_responseV2(
            500, {"message": "Internal Server Error", "details": str(e)}
        )
```

**Query Endpoint Example** (using `list_events_for_job`):

```python
def handler(event: APIGatewayProxyEventV2, _context: Context) -> APIGatewayProxyResponseV2:
    """
    API Gateway Lambda handler to list events for a job.
    GET /jobs/{job_id}/events
    """
    path_params = event.get("pathParameters") or {}
    job_id = path_params.get("job_id")

    if not job_id or not job_id.strip():
        print("ERROR: 'job_id' path parameter is required")
        return HttpResponse.api_gateway_responseV2(
            400, {"message": "Bad Request", "details": "Missing 'job_id' in path"}
        )

    try:
        events_list = event_store_client.list_events_for_job(job_id)
        events_dicts = [event.model_dump() for event in events_list]
        return HttpResponse.api_gateway_responseV2(200, {"events": events_dicts})

    except Exception as e:
        print(f"ERROR: Failed to list job events for job ID {job_id}: {e}")
        return HttpResponse.api_gateway_responseV2(
            500, {"message": "Internal Server Error", "details": str(e)}
        )
```

### Model Guidelines

**Responsibilities**:

- Validate incoming request data using Pydantic
- Provide type-safe request objects

**Template**:

```python
from pydantic import BaseModel


class IncomingCancelJobRequest(BaseModel):
    job_id: str
    job_name: str
```

---

## Implementing Workers

### Worker Structure

```
<worker_name>/
├── <worker_name>.py        # Lambda handler
└── requirements.txt         # Lambda dependencies
```

### Worker Handler Guidelines

**Responsibilities**:

- Parse SQS event
- Iterate SQS records
- Reconstitute events using `EventBase.from_eventbridge_sqs_record()`
- Process business logic
- Optionally publish new events using `Event.from_data()` factory method
- Handle errors (transient vs permanent)

**Template**:

```python
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

import boto3

from services.__errors.error_event_already_raised import ErrorEventAlreadyRaisedException
from services.__events.event_base import EventBase
from services.__events.event_store_client import EventStoreClient
from services.__events.job_cancelled_event import JobCancelledEvent, JobCancelledEventData

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource
else:
    Context = Any
    SQSEvent = Dict[str, Any]
    DynamoDBServiceResource = Any


dynamodb_client: DynamoDBServiceResource = boto3.resource("dynamodb")  # type: ignore

TABLE_NAME = os.environ.get("TABLE_NAME")
if not TABLE_NAME:
    raise ValueError("'TABLE_NAME' environment variable not set.")

event_store_client = EventStoreClient(dynamodb_client, TABLE_NAME)


def handler(sqs_event: SQSEvent, _context: Context) -> None:
    """
    SQS-triggered Lambda for JobCancelledEvent events.
    """
    for sqs_record in sqs_event["Records"]:
        try:
            incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, JobCancelledEvent)
            print(f"INFO: JobCancelledEvent received with job ID: {incoming_event.idempotencyKey}")

        except Exception as e:
            # When parsing fails, log and remove the message from the queue (poison message)
            print(f"ERROR: Invalid SQS record: {e}. The record will be removed from the queue.")
            continue

        incoming_event_data: JobCancelledEventData = incoming_event.eventData

        # Perform business logic here
        # ...

        # Optionally publish new event
        # new_event = SomeOtherEvent(...)
        # event_store_client.raise_event(new_event)

        try:
            # Process the event (e.g., update database, call external API)
            # If successful, message is removed from queue
            print(f"SUCCESS: Processed JobCancelledEvent for job ID: {incoming_event.idempotencyKey}")

        except ErrorEventAlreadyRaisedException:
            # When the event was already raised, log and remove the message from the queue
            print(f"INFO: Idempotent request received for job ID: {incoming_event.idempotencyKey}")
            continue

        except Exception as e:
            # When other errors occur, log and re-raise to keep the message in the queue for retry
            # Only transient errors should be re-raised
            record_id = sqs_record.get("messageId", "Unknown")
            print(f"ERROR: Error processing SQS record {record_id}: {e}")
            raise
```

### Handling Multiple Event Types in Workers

When a worker listens to multiple event types (e.g., `CompleteAllTasksWorker`), the handler must be able to reconstitute any of the expected event types:

```python
def handler(sqs_event: SQSEvent, _context: Context) -> None:
    """
    SQS-triggered Lambda for task execution events.
    Listens to TASK_FOO_EXECUTED_EVENT, TASK_QUX_EXECUTED_EVENT, and TASK_BAR_EXECUTED_EVENT.
    """
    for sqs_record in sqs_event["Records"]:
        try:
            # Try to reconstitute as any of the expected event types
            incoming_event = None
            for event_class in [TaskFooExecutedEvent, TaskQuxExecutedEvent, TaskBarExecutedEvent]:
                try:
                    incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, event_class)
                    print(f"INFO: {event_class.__name__} received with job ID: {incoming_event.idempotencyKey}")
                    break
                except Exception:
                    # Try next event type
                    continue

            if incoming_event is None:
                # When parsing fails for all event types, remove poison message
                print("ERROR: Invalid SQS record: Could not parse as any expected event type.")
                continue

        except Exception as e:
            # When parsing fails, remove poison message
            print(f"ERROR: Invalid SQS record: {e}")
            continue

        # Process event (all event types share the same structure)
        job_id = incoming_event.eventData.job_id
        # ... business logic ...
```

**Pattern:**
1. Iterate through expected event classes
2. Try to reconstitute using each class
3. Break on first successful reconstitution
4. If all fail, treat as poison message and continue

### Worker Error Handling

Workers should only re-raise exceptions for transient errors. Use `ErrorBase.safe_is_transient()` to check:

```python
from services.__errors.error_base import ErrorBase

try:
    # Process event
    pass
except Exception as e:
    if ErrorBase.safe_is_transient(e):
        # Re-raise transient errors for retry
        raise
    else:
        # Log permanent errors and continue (remove message from queue)
        print(f"ERROR: Permanent error processing record: {e}")
        continue
```

---

## Error Handling

### Exception-Based Pattern

Always use custom exceptions instead of generic exceptions:

```python
# ✅ Good
try:
    event_store_client.raise_event(event)
except ErrorEventAlreadyRaisedException:
    # Handle duplicate event
    pass
except ErrorTransient as e:
    # Handle transient error (retry)
    raise
except ErrorPermanent as e:
    # Handle permanent error (don't retry)
    pass

# ❌ Bad
try:
    event_store_client.raise_event(event)
except Exception as e:
    # Too generic
    pass
```

### Error Types

- **ErrorInvalidArgumentsException**: Validation failures, malformed input (non-transient)
- **ErrorEventAlreadyRaisedException**: Event already exists (non-transient)
- **ErrorTransient**: Unexpected errors that should be retried (transient)
- **ErrorPermanent**: Unexpected errors that should not be retried (non-transient)

### Transient vs Non-Transient

- **Transient**: Should retry (e.g., network errors, throttling, temporary failures)
- **Non-Transient**: Don't retry (e.g., validation errors, duplicate events)

Workers only retry transient failures:

```python
try:
    process_event(event)
except Exception as e:
    if ErrorBase.safe_is_transient(e):
        # Re-raise for retry
        raise
    else:
        # Log and continue (remove from queue)
        continue
```

---

## Testing Guidelines

### ⚠️ MANDATORY: Create Tests for Every Feature

**CRITICAL REQUIREMENT**: When developing any new feature, you MUST create comprehensive test files for ALL components. This includes:

- ✅ **Events**: Every event class must have a test file
- ✅ **Handlers**: Every handler must have a test file
- ✅ **Clients**: Every client must have a test file (if applicable)

**Test files are mandatory and must be created as part of the feature implementation, not as an afterthought.**

### Test File Naming

- **Format**: `{component_name}_test.py` (e.g., `job_created_event_test.py`, `create_job_endpoint_test.py`)
- **Location**: Co-located with the component being tested or in a `tests/` subdirectory
- **Never use**: `.spec.py` or `test_{name}.py` (use `{name}_test.py` format)

### Test Structure and Organization

Tests are organized into logical sections separated by comment blocks:

```python
import pytest
from datetime import datetime

from services.__events.job_created_event import JobCreatedEvent, JobCreatedEventData


class TestJobCreatedEvent:
    """
    ************************************************************
    * Test EventBase.from_dynamodb_record edge cases
    ************************************************************
    """
    def test_does_not_raise_if_input_dynamodb_record_is_valid(self):
        # Test implementation
        pass

    """
    ************************************************************
    * Test EventBase.from_eventbridge_sqs_record edge cases
    ************************************************************
    """
    def test_does_not_raise_if_input_sqs_record_is_valid(self):
        # Test implementation
        pass

    """
    ************************************************************
    * Test event creation edge cases
    ************************************************************
    """
    def test_creates_event_with_valid_data(self):
        # Test implementation
        pass

    """
    ************************************************************
    * Test expected results
    ************************************************************
    """
    def test_event_has_correct_event_name(self):
        # Test implementation
        pass
```

### Comment Block Format

Use multi-line comment blocks with asterisks to separate test sections:

```python
"""
************************************************************
* Section Title
************************************************************
"""
```

**Sections typically include:**

1. **Edge cases** - Test invalid inputs (None, empty, blank, invalid types)
2. **Internal logic** - Test method calls, parameters, propagation
3. **Expected results** - Test successful execution paths

### Test Naming Conventions

**Format**: Use descriptive, natural language test names:

```python
# ✅ Good - Descriptive, natural language
def test_does_not_raise_if_input_job_id_is_valid(self):
    ...

def test_raises_error_if_input_job_id_is_none(self):
    ...

def test_calls_event_store_client_raise_event_single_time(self):
    ...

def test_responds_with_400_bad_request_if_input_body_is_none(self):
    ...
```

**Patterns:**

- **Positive cases**: `test_does_not_raise_if...`, `test_creates_event_with...`
- **Negative cases**: `test_raises_error_if...`, `test_returns_400_if...`
- **Behavior verification**: `test_calls_X_single_time`, `test_calls_X_with_expected_input`
- **Response verification**: `test_responds_with_status_code_X`, `test_returns_expected_response`

### Mock Functions

**Naming Convention:**

- **Prefix**: `build_mock` (e.g., `build_mock_api_event`, `build_mock_event_store_client`)
- **Suffixes**:
  - `_succeeds` - Returns success result
  - `_fails` - Returns failure result
  - `_raises` - Raises exception

**Organization:**
Mocks are organized in comment blocks at the top of the test file:

```python
"""
************************************************************
* Mock services
************************************************************
"""


def build_mock_event_store_client_succeeds():
    mock_client = MagicMock()
    mock_client.raise_event = MagicMock(return_value=None)
    return mock_client


def build_mock_event_store_client_raises(error: Exception):
    mock_client = MagicMock()
    mock_client.raise_event = MagicMock(side_effect=error)
    return mock_client
```

### Test Coverage Requirements

#### Edge Cases (Must Test)

For every input parameter, test:

1. **None** - `None`
2. **Empty string** - `""`
3. **Blank string** - `"      "`
4. **Invalid type** - Wrong type
5. **Missing required fields** - Missing fields in Pydantic models
6. **Length constraints** - Values below minimum length
7. **Type constraints** - Wrong literal values

**Example:**

```python
def test_raises_validation_error_if_job_id_is_none(self):
    with pytest.raises(ValidationError):
        JobCreatedEventData(job_id=None, job_name="Test", job_status="CREATED")


def test_raises_validation_error_if_job_id_is_empty(self):
    with pytest.raises(ValidationError):
        JobCreatedEventData(job_id="", job_name="Test", job_status="CREATED")
```

#### Internal Logic (Must Test)

1. **Method calls**: Verify methods are called with correct parameters
2. **Call counts**: Verify methods are called the expected number of times
3. **Error propagation**: Verify exceptions are raised correctly
4. **Service interactions**: Verify services/clients are called correctly

**Example:**

```python
def test_calls_event_store_client_raise_event_single_time(self, mock_event_store_client):
    handler(mock_event, mock_context)
    mock_event_store_client.raise_event.assert_called_once()


def test_calls_event_store_client_raise_event_with_expected_event(self, mock_event_store_client):
    handler(mock_event, mock_context)
    call_args = mock_event_store_client.raise_event.call_args[0][0]
    assert isinstance(call_args, JobCreatedEvent)
    assert call_args.eventName == "JOB_CREATED_EVENT"
```

#### Expected Results (Must Test)

1. **Success paths**: Verify successful execution returns expected results
2. **Failure paths**: Verify different exceptions return appropriate responses
3. **Response formats**: Verify HTTP responses, event structures, etc.

**Example:**

```python
def test_returns_202_accepted_on_success(self):
    response = handler(mock_event, mock_context)
    assert response["statusCode"] == 202
    assert json.loads(response["body"])["message"] == "Accepted"


def test_returns_400_bad_request_on_validation_error(self):
    invalid_event = {**mock_event, "body": "invalid json"}
    response = handler(invalid_event, mock_context)
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["message"] == "Bad Request"
```

### Exception Testing

Always test exception handling:

```python
# Test exception is raised
def test_raises_error_event_already_raised_exception_on_duplicate(self):
    with pytest.raises(ErrorEventAlreadyRaisedException):
        event_store_client.raise_event(duplicate_event)


# Test exception is caught and handled
def test_returns_202_on_error_event_already_raised_exception(self, mock_event_store_client):
    mock_event_store_client.raise_event.side_effect = ErrorEventAlreadyRaisedException(
        Exception("Duplicate"), mock_event
    )
    response = handler(mock_event, mock_context)
    assert response["statusCode"] == 202
```

### Type Safety Testing

When testing type validation, you can test both the factory method and direct instantiation:

```python
def test_from_data_raises_validation_error_if_invalid_data(self):
    with pytest.raises(ValidationError):
        JobCreatedEvent.from_data(
            job_id="",  # Invalid: empty string
            job_name="test",
            job_status="CREATED",
        )

def test_direct_instantiation_raises_validation_error_if_wrong_type(self):
    with pytest.raises(ValidationError):
        JobCreatedEvent(
            idempotencyKey="test",
            createdAt=datetime.now().isoformat(),
            eventData="not a JobCreatedEventData",  # Wrong type
        )
```

### Testing `from_data()` Factory Methods

When testing `from_data()` factory methods, ensure coverage of:

1. **Valid data creation**:
   ```python
   def test_from_data_creates_event_with_valid_data(self):
       event = JobCreatedEvent.from_data(
           job_id="JOB-123",
           job_name="Test Job",
           job_status="CREATED",
       )
       assert event.eventName == "JOB_CREATED_EVENT"
       assert isinstance(event.eventData, JobCreatedEventData)
       assert event.eventData.job_id == "JOB-123"
   ```

2. **Auto-generated fields**:
   ```python
   def test_from_data_auto_generates_idempotency_key(self):
       event = JobCreatedEvent.from_data(
           job_id="JOB-123",
           job_name="Test Job",
           job_status="CREATED",
       )
       assert event.idempotencyKey == "JOB_ID#JOB-123"

   def test_from_data_auto_generates_created_at(self):
       event = JobCreatedEvent.from_data(
           job_id="JOB-123",
           job_name="Test Job",
           job_status="CREATED",
       )
       # Should be ISO format timestamp
       assert "T" in event.createdAt
       assert event.createdAt.endswith("Z") or "+" in event.createdAt or event.createdAt.count(":") == 2
   ```

3. **Validation errors**:
   ```python
   def test_from_data_raises_validation_error_if_job_id_is_empty(self):
       with pytest.raises(ValidationError):
           JobCreatedEvent.from_data(
               job_id="",  # Invalid
               job_name="Test Job",
               job_status="CREATED",
           )
   ```

### Test Organization Checklist

For each component, ensure tests cover:

- [ ] **Edge cases**: None, empty, blank, invalid types
- [ ] **Validation**: All validation rules (length, format, type)
- [ ] **Internal logic**: Method calls, parameters, propagation
- [ ] **Success paths**: Expected results for valid inputs
- [ ] **Failure paths**: All exception types and handling
- [ ] **Response formats**: HTTP status codes, event structures
- [ ] **Type safety**: Instance checks, type validation

---

## Infrastructure Guidelines

### Overview

This project uses **reusable CDK constructs** to simplify infrastructure creation:

- **`ApiEndpointConstruct`**: Creates API Gateway endpoints with Lambda functions
- **`WorkerConstruct`**: Creates workers with SQS queues, DLQ, Lambda functions, and EventBridge rules
- **`DynamoDbConstruct`**: Creates DynamoDB table with streams
- **`EventDriverConstruct`**: Creates EventBridge bus and pipe from DynamoDB stream

**CRITICAL**: Always use these reusable constructs. Do NOT create custom constructs unless absolutely necessary.

### Code Asset Paths

Before deploying, Lambda code must be packaged into ZIP files. The build process creates these in `.dist/services/{service_name}.zip`.

**Code asset paths in infrastructure must point to these ZIP files:**
- ✅ `".dist/services/create_job_endpoint.zip"`
- ❌ `"services/create_job_endpoint"` (source directory - only works in development)

### Creating API Infrastructure

Use `ApiEndpointConstruct` with `ApiEndpointProps` dataclass:

```python
from custom_constructs.api_endpoint_construct import ApiEndpointConstruct, ApiEndpointProps
from custom_constructs.api_construct import ApiConstruct
from custom_constructs.dynamodb_construct import DynamoDbConstruct

# In main_stack.py:

# 1. Create API construct (one per stack)
api = ApiConstruct(self, "Api", base_name=f"{stack_id}-apigw")

# 2. Create endpoint using ApiEndpointConstruct
cancel_job_endpoint = ApiEndpointConstruct(
    self,
    "CancelJobEndpoint",
    http_api=api.http_api,
    props=ApiEndpointProps(
        base_name=f"{stack_id}-cancel-job-endpoint",
        method="POST",
        path="/jobs/{job_id}/cancel",
        code_asset_path=".dist/services/cancel_job_endpoint.zip",  # Must be ZIP file
        handler="cancel_job_endpoint.handler",  # Format: "module.function"
        environment={"TABLE_NAME": ddb_construct.table.table_name},
        timeout_sec=20,  # Default: 20 seconds (max: 29 for API Gateway)
    ),
)

# 3. Grant DynamoDB permissions (MUST be done after construct creation)
ddb_construct.table.grant_read_write_data(cancel_job_endpoint.lambda_function)
```

**ApiEndpointProps fields:**

- `base_name` (str): Base name for Lambda function (e.g., `"my-stack-cancel-job-endpoint"`)
- `method` (str): HTTP method (`"GET"`, `"POST"`, `"PUT"`, `"DELETE"`, etc.)
- `path` (str): API path (e.g., `"/jobs"` or `"/jobs/{job_id}/cancel"`)
- `code_asset_path` (str): Path to ZIP file (e.g., `".dist/services/cancel_job_endpoint.zip"`)
- `handler` (str): Lambda handler in format `"module.function"` (e.g., `"cancel_job_endpoint.handler"`)
- `runtime` (Runtime): Lambda runtime (default: `Runtime.PYTHON_3_12`)
- `memory_mb` (int): Lambda memory in MB (default: 256)
- `timeout_sec` (int): Lambda timeout in seconds (default: 20, max: 29 for API Gateway)
- `environment` (Optional[Dict[str, str]]): Environment variables (default: `None`)
- `log_retention_days` (RetentionDays): CloudWatch log retention (default: `ONE_WEEK`)

### Creating Worker Infrastructure

Use `WorkerConstruct` with `WorkerProps` dataclass:

```python
from custom_constructs.worker_construct import WorkerConstruct, WorkerProps
from aws_cdk import aws_events, aws_events_targets

# In main_stack.py:

# 1. Create worker using WorkerConstruct
cancel_job_worker = WorkerConstruct(
    self,
    "CancelJobWorker",
    bus=event_driver.bus,
    props=WorkerProps(
        base_name=f"{stack_id}-cancel-job-worker",
        match_event_names=["JOB_CANCELLED_EVENT"],  # List of event names to listen to
        source="app.inventory",  # Must match EventDriverConstruct source
        detail_type="DynamoDBItemChange",  # Must match EventDriverConstruct detail_type
        code_asset_path=".dist/services/cancel_job_worker.zip",  # Must be ZIP file
        handler="cancel_job_worker.handler",  # Format: "module.function"
        environment={"TABLE_NAME": ddb_construct.table.table_name},
        lambda_timeout_sec=60,  # Default: 30 seconds
        memory_mb=256,  # Default: 256 MB
    ),
)

# 2. Grant DynamoDB permissions (MUST be done after construct creation)
ddb_construct.table.grant_read_write_data(cancel_job_worker.lambda_function)
```

**WorkerProps fields:**

- `base_name` (str): Base name for resources (e.g., `"my-stack-cancel-job-worker"`)
- `match_event_names` (List[str]): Event names to listen to (e.g., `["JOB_CANCELLED_EVENT"]`)
- `source` (str): EventBridge source (must match `EventDriverConstruct.source`)
- `detail_type` (str): EventBridge detail type (must match `EventDriverConstruct.detail_type`)
- `code_asset_path` (str): Path to ZIP file (e.g., `".dist/services/cancel_job_worker.zip"`)
- `handler` (str): Lambda handler in format `"module.function"` (e.g., `"cancel_job_worker.handler"`)
- `runtime` (Runtime): Lambda runtime (default: `Runtime.PYTHON_3_12`)
- `memory_mb` (int): Lambda memory in MB (default: 256)
- `lambda_timeout_sec` (int): Lambda timeout in seconds (default: 30)
- `visibility_timeout_sec` (int): SQS visibility timeout in seconds (default: 30)
- `retention_days` (int): SQS message retention in days (default: 4)
- `enable_dlq` (bool): Enable dead letter queue (default: `True`)
- `environment` (Optional[Dict[str, str]]): Environment variables (default: `None`)
- `log_retention_days` (RetentionDays): CloudWatch log retention (default: `ONE_WEEK`)

**What WorkerConstruct creates:**

- SQS Queue (name: `"{base_name}-queue"`)
- Dead Letter Queue (name: `"{base_name}-dlq"`, if `enable_dlq=True`)
- CloudWatch Log Group (name: `"/aws/lambda/{base_name}-fn"`)
- Lambda Function (name: `"{base_name}-fn"`)
- EventBridge Rule (routes matching events to SQS queue)

### Workers Listening to Multiple Events

When a worker needs to listen to multiple event types (e.g., `CompleteAllTasksWorker`), use this pattern:

```python
# 1. Create worker with WorkerConstruct (listens to first event)
complete_all_tasks_worker = WorkerConstruct(
    self,
    "CompleteAllTasksWorker",
    bus=event_driver.bus,
    props=WorkerProps(
        base_name=f"{stack_id}-complete-all-tasks-worker",
        match_event_names=["TASK_FOO_EXECUTED_EVENT"],  # First event
        source="app.inventory",
        detail_type="DynamoDBItemChange",
        code_asset_path=".dist/services/complete_all_tasks_worker.zip",
        handler="complete_all_tasks_worker.handler",
        environment={"TABLE_NAME": ddb_construct.table.table_name},
    ),
)
ddb_construct.table.grant_read_write_data(complete_all_tasks_worker.lambda_function)

# 2. Add additional EventBridge Rules for other events (all route to same SQS queue)
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
    targets=[aws_events_targets.SqsQueue(complete_all_tasks_worker.sqs_queue)],
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
    targets=[aws_events_targets.SqsQueue(complete_all_tasks_worker.sqs_queue)],
)
```

**Important**: The worker handler must be able to handle multiple event types. See [Handling Multiple Event Types in Workers](#handling-multiple-event-types-in-workers) section.

### Wiring in Main Stack

Complete example of wiring infrastructure in `main_stack.py`:

```python
from custom_constructs.api_construct import ApiConstruct
from custom_constructs.api_endpoint_construct import ApiEndpointConstruct, ApiEndpointProps
from custom_constructs.dynamodb_construct import DynamoDbConstruct
from custom_constructs.event_driver_construct import EventDriverConstruct
from custom_constructs.worker_construct import WorkerConstruct, WorkerProps

class MainStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        stack_id = construct_id

        # 1. Common infrastructure (created once per stack)
        ddb_construct = DynamoDbConstruct(self, "DynamoDb", base_name=f"{stack_id}-ddb")
        event_driver = EventDriverConstruct(
            self,
            "EventDriver",
            base_name=f"{stack_id}-event-driver",
            table_stream_arn=str(ddb_construct.table_stream_arn),
            source="app.inventory",
            detail_type="DynamoDBItemChange",
        )

        # 2. API infrastructure
        api = ApiConstruct(self, "Api", base_name=f"{stack_id}-apigw")

        # Create endpoints
        create_job_endpoint = ApiEndpointConstruct(
            self,
            "CreateJobEndpoint",
            http_api=api.http_api,
            props=ApiEndpointProps(
                base_name=f"{stack_id}-create-job-endpoint",
                method="POST",
                path="/jobs",
                code_asset_path=".dist/services/create_job_endpoint.zip",
                handler="create_job_endpoint.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(create_job_endpoint.lambda_function)

        # 3. Worker infrastructure
        cancel_job_worker = WorkerConstruct(
            self,
            "CancelJobWorker",
            bus=event_driver.bus,
            props=WorkerProps(
                base_name=f"{stack_id}-cancel-job-worker",
                match_event_names=["JOB_CANCELLED_EVENT"],
                source="app.inventory",
                detail_type="DynamoDBItemChange",
                code_asset_path=".dist/services/cancel_job_worker.zip",
                handler="cancel_job_worker.handler",
                environment={"TABLE_NAME": ddb_construct.table.table_name},
            ),
        )
        ddb_construct.table.grant_read_write_data(cancel_job_worker.lambda_function)
```

---

## Best Practices

### 1. Always Validate Input

```python
# ✅ Good
try:
    incoming_request = IncomingCreateJobRequest.model_validate_json(body_str)
except ValidationError as e:
    return HttpResponse.api_gateway_responseV2(400, {"message": "Bad Request", "details": str(e)})

# ❌ Bad
# Assume input is valid
```

### 2. Use Custom Exceptions, Not Generic Ones

```python
# ✅ Good
except ErrorEventAlreadyRaisedException:
    # Handle duplicate event
    pass
except ErrorTransient as e:
    # Handle transient error
    raise

# ❌ Bad
except Exception as e:
    # Too generic
    pass
```

### 3. Log at Entry and Exit Points

```python
print(f"INFO: Processing event with job ID: {job_id}")
# ... logic ...
print(f"SUCCESS: Event processed successfully for job ID: {job_id}")
print(f"ERROR: Failed to process event for job ID: {job_id}: {e}")
```

### 4. Use Type Hints with TYPE_CHECKING

```python
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from aws_lambda_typing.events import APIGatewayProxyEventV2
else:
    APIGatewayProxyEventV2 = Dict[str, Any]
```

### 5. Use Pydantic for Validation

```python
# ✅ Good
class IncomingRequest(BaseModel):
    job_id: str
    job_name: str

request = IncomingRequest.model_validate_json(body_str)

# ❌ Bad
# Manual validation
```

### 6. One Responsibility Per Function

- **Handler**: AWS I/O handling, validation, error mapping
- **EventStoreClient**: External system interaction (DynamoDB)
- **Event Classes**: Event structure and validation

---

## Common Pitfalls

### ❌ Don't: Use Generic Exceptions

```python
# Bad
try:
    event_store_client.raise_event(event)
except Exception as e:
    # Too generic
    pass
```

### ✅ Do: Use Specific Exceptions

```python
# Good
try:
    event_store_client.raise_event(event)
except ErrorEventAlreadyRaisedException:
    # Handle duplicate event
    pass
except ErrorTransient as e:
    # Handle transient error
    raise
```

### ❌ Don't: Skip Validation

```python
# Bad
event = JobCreatedEvent.from_data(
    job_id=unvalidated_data["job_id"],
    job_name=unvalidated_data["job_name"],
    job_status=unvalidated_data["job_status"],
)
# This bypasses Pydantic validation and can lead to invalid data
```

### ✅ Do: Validate First

```python
# Good
try:
    incoming_request = IncomingCreateJobRequest.model_validate_json(body_str)
except ValidationError as e:
    return HttpResponse.api_gateway_responseV2(400, {"message": "Bad Request", "details": str(e)})

event = JobCreatedEvent.from_data(
    job_id=incoming_request.job_id,
    job_name=incoming_request.job_name,
    job_status="CREATED",
)
# from_data() ensures proper validation and auto-generates idempotencyKey and createdAt
```

### ❌ Don't: Re-raise All Exceptions in Workers

```python
# Bad
try:
    process_event(event)
except Exception as e:
    # Re-raises all exceptions, including permanent ones
    raise
```

### ✅ Do: Only Re-raise Transient Errors

```python
# Good
try:
    process_event(event)
except Exception as e:
    if ErrorBase.safe_is_transient(e):
        # Only re-raise transient errors
        raise
    else:
        # Log permanent errors and continue
        print(f"ERROR: Permanent error: {e}")
        continue
```

---

## Quick Reference

### Critical Patterns

#### 1. Exception-Based Error Handling (Never Use Generic Exceptions)

```python
# ✅ ALWAYS use specific exceptions
try:
    event_store_client.raise_event(event)
except ErrorEventAlreadyRaisedException:
    # Handle duplicate event
    pass
except ErrorTransient as e:
    # Handle transient error (retry)
    raise
except ErrorPermanent as e:
    # Handle permanent error (don't retry)
    pass

# ❌ NEVER use generic exceptions
except Exception as e:
    pass
```

#### 2. Logging Pattern

```python
print(f"INFO: Processing event with job ID: {job_id}")
# ... logic ...
print(f"SUCCESS: Event processed successfully for job ID: {job_id}")
print(f"ERROR: Failed to process event for job ID: {job_id}: {e}")
```

#### 3. HTTP Response Pattern

```python
# Use HttpResponse class
return HttpResponse.api_gateway_responseV2(202, {"message": "Accepted"})  # 202
return HttpResponse.api_gateway_responseV2(200, {"data": data})  # 200
return HttpResponse.api_gateway_responseV2(400, {"message": "Bad Request"})  # 400
return HttpResponse.api_gateway_responseV2(500, {"message": "Internal Server Error"})  # 500
```

#### 4. Worker Error Handling Pattern

```python
for sqs_record in sqs_event["Records"]:
    try:
        incoming_event = EventBase.from_eventbridge_sqs_record(sqs_record, EventClass)
        # Process event
    except Exception as e:
        # When parsing fails, remove poison message
        print(f"ERROR: Invalid SQS record: {e}")
        continue

    try:
        # Process business logic
        pass
    except ErrorEventAlreadyRaisedException:
        # Handle duplicate event
        continue
    except Exception as e:
        # Only re-raise transient errors
        if ErrorBase.safe_is_transient(e):
            raise
        else:
            continue
```

#### 5. Event Reconstitution in Workers

```python
# Reconstitute event from SQS/EventBridge payload
incoming_event = EventBase.from_eventbridge_sqs_record(
    sqs_record, JobCreatedEvent
)

# Access event data
event_data: JobCreatedEventData = incoming_event.eventData
```

### Quick Decision Tree

**Adding a new feature?**

1. **Need to expose HTTP endpoint?** → Create API endpoint (Handler + Infrastructure)
2. **Need to process events asynchronously?** → Create Worker (Handler + Infrastructure)
3. **Need to represent state change?** → Create Event (Pydantic model extending EventBase)
4. **Need to filter events?** → Create EventBridge Rule in Worker construct

**Error handling?**

- **Validation error?** → Return 400 Bad Request
- **Duplicate event?** → Return 202 Accepted (idempotent)
- **Transient error?** → Re-raise exception (for retry)
- **Permanent error?** → Log and continue (don't retry)

**HTTP response?**

- **Event published successfully?** → `HttpResponse.api_gateway_responseV2(202, {...})`
- **Validation failed?** → `HttpResponse.api_gateway_responseV2(400, {...})`
- **Unexpected error?** → `HttpResponse.api_gateway_responseV2(500, {...})`

### Python Requirements

- **Type hints**: All functions must have explicit type hints
- **Strict typing**: Use `pyright` or `mypy` in strict mode
- **Pydantic validation**: All inputs validated using Pydantic models
- **Exception handling**: Use custom exceptions, not generic ones
- **TYPE_CHECKING guard**: Use `TYPE_CHECKING` for type-only imports

### Checklists

#### Event Creation Checklist {#event-creation-checklist}

- [ ] Extend `EventBase` (Pydantic BaseModel)
- [ ] Define `eventName` class attribute (format: `UPPER_SNAKE_CASE`, e.g., `"JOB_CANCELLED_EVENT"`)
- [ ] Create Pydantic model for `eventData` (naming: `{EventName}Data`, e.g., `JobCancelledEventData`)
- [ ] Add `from __future__ import annotations` at top of file (for forward references)
- [ ] Implement `from_data()` class method factory that:
  - [ ] Accepts event data fields as parameters (not `EventBase` fields)
  - [ ] Creates `eventData` instance from parameters
  - [ ] Auto-generates `idempotencyKey` using format `JOB_ID#{job_id}` (or custom format if needed)
  - [ ] Auto-generates `createdAt` using `datetime.now().isoformat()`
  - [ ] Returns fully constructed event instance with type hint `-> EventName`
  - [ ] Includes docstring explaining usage
- [ ] Create test file `{event_name}_test.py` with comprehensive coverage:
  - [ ] Test `from_data()` with valid data
  - [ ] Test `from_data()` with invalid data (validation errors)
  - [ ] Test auto-generated `idempotencyKey` format
  - [ ] Test auto-generated `createdAt` is ISO format
  - [ ] Test direct instantiation validation (if applicable)

#### API Implementation Checklist {#api-implementation-checklist}

- [ ] Create handler file with proper type hints
- [ ] Create Pydantic model for incoming request
- [ ] Validate input using Pydantic
- [ ] Build domain event using `Event.from_data()` factory method
- [ ] Publish event via EventStoreClient
- [ ] Handle exceptions and map to HTTP status codes
- [ ] Create infrastructure construct
- [ ] Wire up in main stack
- [ ] Create handler test file with edge cases, internal logic, and expected results

#### Worker Implementation Checklist {#worker-implementation-checklist}

- [ ] Create handler file with proper type hints
- [ ] Iterate SQS records
- [ ] Reconstitute events using `EventBase.from_eventbridge_sqs_record()`
- [ ] Process business logic
- [ ] Optionally publish new events
- [ ] Handle exceptions (only re-raise transient errors)
- [ ] Create infrastructure construct (SQS + DLQ + Lambda + Rule)
- [ ] Wire up in main stack
- [ ] Create handler test file with edge cases, internal logic, and expected results

#### Infrastructure Checklist {#infrastructure-checklist}

**API Infrastructure:**

- [ ] Use `ApiEndpointConstruct` with `ApiEndpointProps` dataclass
- [ ] Set `base_name` following naming convention: `"{stack_id}-{endpoint-name}"`
- [ ] Set `method` to HTTP method (e.g., `"POST"`, `"GET"`)
- [ ] Set `path` to API path (e.g., `"/jobs"` or `"/jobs/{job_id}/cancel"`)
- [ ] Set `code_asset_path` to ZIP file: `".dist/services/{endpoint_name}.zip"`
- [ ] Set `handler` in format `"{module}.{function}"` (e.g., `"create_job_endpoint.handler"`)
- [ ] Set `TABLE_NAME` in `environment` dict
- [ ] Grant DynamoDB read/write permissions using `table.grant_read_write_data(endpoint.lambda_function)`
- [ ] Use appropriate timeout (default: 20 seconds, max: 29 seconds for API Gateway)

**Worker Infrastructure:**

- [ ] Use `WorkerConstruct` with `WorkerProps` dataclass
- [ ] Set `base_name` following naming convention: `"{stack_id}-{worker-name}"`
- [ ] Set `match_event_names` to list of event names (e.g., `["JOB_CANCELLED_EVENT"]`)
- [ ] Set `source` and `detail_type` to match `EventDriverConstruct` values
- [ ] Set `code_asset_path` to ZIP file: `".dist/services/{worker_name}.zip"`
- [ ] Set `handler` in format `"{module}.{function}"` (e.g., `"cancel_job_worker.handler"`)
- [ ] Set `TABLE_NAME` in `environment` dict
- [ ] Grant DynamoDB read/write permissions using `table.grant_read_write_data(worker.lambda_function)`
- [ ] If listening to multiple events, add additional `aws_events.Rule` instances routing to same SQS queue
- [ ] Use appropriate timeout (default: 30 seconds, configurable via `lambda_timeout_sec`)

#### Error Handling Checklist

- [ ] Use custom exceptions, never generic `Exception`
- [ ] Check `ErrorBase.safe_is_transient()` for retry logic
- [ ] Only re-raise transient errors in workers
- [ ] Map business errors to appropriate HTTP status codes
- [ ] Log all errors with context

### Key Files to Reference

When implementing features, reference these existing files:

- **Event Example**: `services/__events/job_created_event.py`
- **API Handler**: `services/create_job_endpoint/create_job_endpoint.py`
- **Worker Handler**: `services/process_step_worker/process_step_worker.py`
- **Event Store Client**: `services/__events/event_store_client.py`
- **Error Classes**: `services/__errors/`
- **API Infrastructure**: `infra/custom_constructs/api_endpoint_construct.py`
- **Worker Infrastructure**: `infra/custom_constructs/worker_construct.py`

---

## Summary

Follow these guidelines to maintain consistency and quality:

1. ✅ Use Event Sourcing pattern
2. ✅ Use Pydantic for validation
3. ✅ Use custom exceptions for error handling
4. ✅ Validate all inputs
5. ✅ Use type hints with strict typing
6. ✅ Write tests for all components (MANDATORY)
7. ✅ Log at entry/exit points
8. ✅ Use idempotency keys for events
9. ✅ Only re-raise transient errors in workers
10. ✅ Follow naming conventions

This template provides a **production-ready foundation** for building event-driven applications on AWS with:

✅ **Event Sourcing** for complete auditability
✅ **Exception-Based Error Handling** with transient flags
✅ **Type-Safe Validation** with Pydantic
✅ **Idempotent event processing**
✅ **Scalable architecture** using serverless AWS services
✅ **Infrastructure as Code** with AWS CDK

The architecture is designed to scale horizontally, handle failures gracefully, and maintain a clear separation between API endpoints and asynchronous workers.
