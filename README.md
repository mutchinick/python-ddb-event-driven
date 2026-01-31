# Python DynamoDB Event-Driven Architecture

A serverless, event-driven job processing system built on AWS using Python, DynamoDB Streams, EventBridge, SQS queues and Lambda functions. This project demonstrates a modern event sourcing pattern with complete event traceability and asynchronous job processing.

## 📋 Table of Contents

- [What It Is](#what-it-is)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Event Flow Example](#event-flow-example)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Build & Deployment](#build--deployment)
- [Testing the System](#testing-the-system)
- [Teardown](#teardown)
- [Development](#development)
- [Technical Details](#technical-details)

## What It Is

This project is a **Proof of Concept (POC)** that demonstrates an **event-driven architecture** using AWS serverless technologies. It showcases:

- **Event Sourcing**: All state changes are captured as immutable events
- **Serverless Architecture**: Built entirely on AWS Lambda functions
- **Asynchronous Processing**: Events flow through multiple processing stages automatically
- **Idempotency**: Safe to retry operations without side effects
- **Observability**: Complete audit trail of all event lifecycle

**⚠️ Important**: This is a demonstration system that doesn't perform actual job processing work. Instead, it focuses on showing how events flow through the system, with each worker simply transitioning jobs to the next state by raising new events.

The system demonstrates a complex event-driven workflow where a job progresses through multiple stages with parallel task execution and completion tracking.

## How It Works

1. **Job Creation**: HTTP POST to `/jobs` endpoint creates a new job
2. **Event Storage**: Job creation event is stored in DynamoDB
3. **Stream Processing**: DynamoDB Streams capture data changes
4. **Event Distribution**: EventBridge distributes events to appropriate workers
5. **Asynchronous Processing**: Lambda workers process jobs through different stages
6. **Parallel Task Execution**: Multiple tasks execute in parallel after step processing
7. **Task Completion Tracking**: System waits for all tasks to complete before finalizing
8. **State Progression**: Each worker moves the job to the next state by raising new events

## Architecture

### Event Flow

```mermaid
graph TB
    Event1[JOB_CREATED_EVENT] --> Event2[STEP_PROCESSED_EVENT]
    Event2 --> Event3[TASK_FOO_EXECUTED_EVENT]
    Event2 --> Event3b[TASK_QUX_EXECUTED_EVENT]
    Event2 --> Event3c[TASK_BAR_EXECUTED_EVENT]
    Event3 --> Event4[ALL_TASKS_COMPLETED_EVENT]
    Event3b --> Event4
    Event3c --> Event4
    Event4 --> Event5[JOB_FINALIZED_EVENT]
```

### Infrastructure Flow

> **Note**: Dead Letter Queues (DLQs) have been removed from this diagram to simplify visibility. Each SQS queue in the system has an associated DLQ for error handling.
>
> **Note**: The DynamoDB Event Store boxes marked with \* represent the same resource, duplicated in the diagram to simplify visibility and reduce line crossings.

```mermaid
graph TB
    Client[REST Client] --> API[CreateJob API Gateway]
    API --> Lambda1[CreateJob Lambda]
    Lambda1 --> DDB1[DynamoDB Event Store *]

    Client2[REST Client] --> API2[ListJobEvents API Gateway]
    API2 --> Lambda2[ListJobEvents Lambda]
    Lambda2 --> DDB1

    DDB1 --> Stream[DynamoDB Streams]
    Stream --> Pipe[EventBridge Pipe]
    Pipe --> EB[EventBridge Bus]

    EB --> Rule1[EventBridge Rule 1]
    Rule1 --> SQS1[SQS Queue 1]
    SQS1 --> Lambda3[ProcessStep Lambda]
    Lambda3 --> DDB2[DynamoDB Event Store *]

    EB --> Rule2[EventBridge Rule 2]
    Rule2 --> SQS2[SQS Queue 2]
    SQS2 --> Lambda4[ExecuteTaskFoo Lambda]
    Lambda4 --> DDB2

    EB --> Rule3[EventBridge Rule 3]
    Rule3 --> SQS3[SQS Queue 3]
    SQS3 --> Lambda5[ExecuteTaskQux Lambda]
    Lambda5 --> DDB2

    EB --> Rule4[EventBridge Rule 4]
    Rule4 --> SQS4[SQS Queue 4]
    SQS4 --> Lambda6[ExecuteTaskBar Lambda]
    Lambda6 --> DDB2

    EB --> Rule5[EventBridge Rule 5]
    Rule5 --> SQS5[SQS Queue 5]
    SQS5 --> Lambda7[CompleteAllTasks Lambda]
    Lambda7 --> DDB2

    EB --> Rule6[EventBridge Rule 6]
    Rule6 --> SQS6[SQS Queue 6]
    SQS6 --> Lambda8[FinalizeJob Lambda]
    Lambda8 --> DDB2

    classDef eventStore fill:#e1f5ff,stroke:#0288d1,stroke-width:3px,color:#000
    class DDB1,DDB2 eventStore
```

### Basic Steps Explained

1. **API Request**: Client creates job via REST API (`POST /jobs`)
2. **Event Storage**: Lambda stores `JOB_CREATED_EVENT` in DynamoDB using `EventStoreClient`
3. **Stream Processing**: DynamoDB Streams captures event, forwards via EventBridge Pipe
4. **Event Routing**: EventBridge Rules route events to SQS queues based on event type
5. **Worker Processing**: Lambda workers poll SQS, use `EventBase.from_eventbridge_sqs_record()` to reconstitute events
6. **Event Chain**:
   - `ProcessStepWorker` listens to `JOB_CREATED_EVENT` → produces `STEP_PROCESSED_EVENT`
   - `ExecuteTaskFooWorker` listens to `STEP_PROCESSED_EVENT` → produces `TASK_FOO_EXECUTED_EVENT`
   - `ExecuteTaskQuxWorker` listens to `STEP_PROCESSED_EVENT` → produces `TASK_QUX_EXECUTED_EVENT`
   - `ExecuteTaskBarWorker` listens to `STEP_PROCESSED_EVENT` → produces `TASK_BAR_EXECUTED_EVENT`
   - `CompleteAllTasksWorker` listens to `TASK_FOO_EXECUTED_EVENT`, `TASK_QUX_EXECUTED_EVENT`, `TASK_BAR_EXECUTED_EVENT` → produces `ALL_TASKS_COMPLETED_EVENT` (only when all three events have been produced)
   - `FinalizeJobWorker` listens to `ALL_TASKS_COMPLETED_EVENT` → produces `JOB_FINALIZED_EVENT`
7. **Workflow Continues**: Each worker processes its event and publishes new events, creating an event-driven workflow chain
8. **Query Events**: Client can query all events for a job via REST API (`GET /jobs/{jobId}/events`)

## Core Components

### Event Store System

**EventBase** (`services/__events/event_base.py`) - Base Pydantic model for domain events

```python
class EventBase(BaseModel):
    idempotencyKey: str
    eventName: str
    createdAt: str
    eventData: Any

    @staticmethod
    def from_dynamodb_record(ddb_record: Dict[str, Any], model: Type[TEvent]) -> TEvent:
        # Parses DynamoDB Stream record

    @staticmethod
    def from_eventbridge_sqs_record(sqs_record: SQSMessage, model: Type[TEvent]) -> TEvent:
        # Parses SQS record with EventBridge payload
```

**EventStoreClient** (`services/__events/event_store_client.py`) - Publishes events to DynamoDB and queries event history

```python
class EventStoreClient:
    def raise_event(self, event: EventBase):
        # Saves event to DynamoDB with idempotency check
        # Raises ErrorEventAlreadyRaisedException if duplicate

    def list_events_for_job(self, job_id: str) -> list[EventBase]:
        # Queries DynamoDB for all events for a specific job
        # Returns list of events sorted by createdAt
        # Raises ErrorTransient or ErrorPermanent on errors
```

### Common Infrastructure

- **DynamoDbConstruct** (`infra/custom_constructs/dynamodb_construct.py`) - Event store table with streams
- **EventDriverConstruct** (`infra/custom_constructs/event_driver_construct.py`) - EventBridge setup with DynamoDB pipe

## Key Components

### 🏗️ Infrastructure (CDK)

- **MainStack**: Orchestrates all AWS resources
- **DynamoDbConstruct**: Event store table with streams enabled
- **EventDriverConstruct**: DynamoDB Stream → EventBridge integration
- **ApiConstruct**: HTTP API Gateway setup
- **ApiEndpointConstruct**: API Gateway endpoint + Lambda function
- **WorkerConstruct**: Lambda function + SQS queue + EventBridge rules

### 🔄 Event System

- **EventBase**: Base Pydantic model for all events with DynamoDB/EventBridge parsing
- **Event Store Client**: Handles event persistence with idempotency
- **Typed Events**:
  - `JobCreatedEvent`
  - `StepProcessedEvent`
  - `TaskFooExecutedEvent`
  - `TaskQuxExecutedEvent`
  - `TaskBarExecutedEvent`
  - `AllTasksCompletedEvent`
  - `JobFinalizedEvent`

### ⚙️ Workers (Lambda Functions)

- **Process Step Worker**: Responds to `JOB_CREATED_EVENT` → creates `STEP_PROCESSED_EVENT`
- **Execute Task Foo Worker**: Responds to `STEP_PROCESSED_EVENT` → creates `TASK_FOO_EXECUTED_EVENT`
- **Execute Task Qux Worker**: Responds to `STEP_PROCESSED_EVENT` → creates `TASK_QUX_EXECUTED_EVENT`
- **Execute Task Bar Worker**: Responds to `STEP_PROCESSED_EVENT` → creates `TASK_BAR_EXECUTED_EVENT`
- **Complete All Tasks Worker**: Responds to all three task events → creates `ALL_TASKS_COMPLETED_EVENT` (only when all three have been produced)
- **Finalize Job Worker**: Responds to `ALL_TASKS_COMPLETED_EVENT` → creates `JOB_FINALIZED_EVENT`

### 🌐 API Endpoints

- **POST /jobs**: Create a new job (triggers the processing pipeline)
- **GET /jobs/{jobId}/events**: List all events for a specific job (retrieve event history)

## Event Flow Example

Here's what happens when you create a job with ID "JOB-123":

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant CreateJob
    participant DDB
    participant EventBridge
    participant ProcessStep
    participant ExecuteTaskFoo
    participant ExecuteTaskQux
    participant ExecuteTaskBar
    participant CompleteAllTasks
    participant FinalizeJob

    Client->>API: POST /jobs {"job_id": "JOB-123", "job_name": "Sample Job"}
    API->>CreateJob: Invoke
    CreateJob->>DDB: Store JOB_CREATED_EVENT
    DDB->>EventBridge: Stream → Pipe
    EventBridge->>ProcessStep: JOB_CREATED_EVENT
    ProcessStep->>DDB: Store STEP_PROCESSED_EVENT
    DDB->>EventBridge: Stream → Pipe
    EventBridge->>ExecuteTaskFoo: STEP_PROCESSED_EVENT
    EventBridge->>ExecuteTaskQux: STEP_PROCESSED_EVENT
    EventBridge->>ExecuteTaskBar: STEP_PROCESSED_EVENT
    ExecuteTaskFoo->>DDB: Store TASK_FOO_EXECUTED_EVENT
    ExecuteTaskQux->>DDB: Store TASK_QUX_EXECUTED_EVENT
    ExecuteTaskBar->>DDB: Store TASK_BAR_EXECUTED_EVENT
    DDB->>EventBridge: Stream → Pipe (all three events)
    EventBridge->>CompleteAllTasks: TASK_FOO_EXECUTED_EVENT
    EventBridge->>CompleteAllTasks: TASK_QUX_EXECUTED_EVENT
    EventBridge->>CompleteAllTasks: TASK_BAR_EXECUTED_EVENT
    Note over CompleteAllTasks: Waits for all three events<br/>before proceeding
    CompleteAllTasks->>DDB: Store ALL_TASKS_COMPLETED_EVENT
    DDB->>EventBridge: Stream → Pipe
    EventBridge->>FinalizeJob: ALL_TASKS_COMPLETED_EVENT
    FinalizeJob->>DDB: Store JOB_FINALIZED_EVENT
    CreateJob->>Client: 202 Accepted
```

## Project Structure

```
python-ddb-event-driven/
├── infra/                              # AWS CDK Infrastructure as Code
│   ├── app.py                          # CDK app entry point
│   ├── main_stack.py                   # Main CloudFormation stack
│   └── custom_constructs/              # Reusable CDK constructs
│       ├── api_construct.py            # HTTP API Gateway setup
│       ├── api_endpoint_construct.py   # API endpoint + Lambda
│       ├── dynamodb_construct.py       # DynamoDB table with streams
│       ├── event_driver_construct.py   # Stream → EventBridge pipe
│       └── worker_construct.py         # Worker Lambda + SQS + EventBridge
│
├── services/                           # Lambda function source code
│   ├── __events/                       # Event system components
│   │   ├── event_base.py               # Base event class & parsing logic
│   │   ├── event_store_client.py       # DynamoDB event persistence
│   │   ├── job_created_event.py        # Job creation event definition
│   │   ├── step_processed_event.py     # Step processed event definition
│   │   ├── task_foo_executed_event.py  # Task Foo executed event definition
│   │   ├── task_qux_executed_event.py # Task Qux executed event definition
│   │   ├── task_bar_executed_event.py # Task Bar executed event definition
│   │   ├── all_tasks_completed_event.py # All tasks completed event definition
│   │   └── job_finalized_event.py      # Job finalized event definition
│   │
│   ├── __errors/                       # Custom exception classes
│   ├── __http_helpers/                 # HTTP response utilities
│   │
│   ├── create_job_endpoint/            # POST /jobs implementation
│   ├── list_job_events_endpoint/        # GET /jobs/{jobId}/events implementation
│   ├── process_step_worker/            # Processes JOB_CREATED_EVENT
│   ├── execute_task_foo_worker/        # Processes STEP_PROCESSED_EVENT
│   ├── execute_task_qux_worker/        # Processes STEP_PROCESSED_EVENT
│   ├── execute_task_bar_worker/        # Processes STEP_PROCESSED_EVENT
│   ├── complete_all_tasks_worker/      # Processes all three task events
│   └── finalize_job_worker/            # Processes ALL_TASKS_COMPLETED_EVENT
│
├── deploy-scripts/                     # Build and deployment automation
│   ├── build-all-lambdas.sh            # Package all Lambda functions
│   ├── build-all-requirements-txt.sh   # Generate requirements.txt files
│   └── build-dist.sh                   # Rapid deployment script
│
├── _restclient/                        # HTTP client examples
├── cdk.json                            # CDK configuration
├── Pipfile                             # Python dependencies & scripts
└── pyproject.toml                      # Code quality tools configuration
```

## Tech Stack

- **Python 3.12+** with pipenv
- **AWS Lambda** - Compute
- **API Gateway v2** - HTTP endpoints
- **DynamoDB** - Event store with streams
- **EventBridge** - Event routing
- **SQS** - Message queues with DLQ
- **AWS CDK** - Infrastructure as Code

## Usage

### Creating Events

All domain events extend `EventBase` (Pydantic BaseModel) and define event data models. For example:

```python
from pydantic import BaseModel
from services.__events.event_base import EventBase

class JobCreatedEventData(BaseModel):
    job_id: str
    job_name: str
    job_status: str

class JobCreatedEvent(EventBase):
    eventName: str = "JOB_CREATED_EVENT"
    eventData: JobCreatedEventData
```

Events are created using the `from_data()` factory method, which automatically generates `idempotencyKey` and `createdAt`:

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

> **Note**: The `from_data()` factory method ensures consistent event creation and prevents tampering with `idempotencyKey` and `createdAt`. For reconstituting events from storage (DynamoDB/EventBridge), use `EventBase.from_dynamodb_record()` or `EventBase.from_eventbridge_sqs_record()`.

> **Reference:** See [GUIDELINES.md](GUIDELINES.md) for detailed event creation patterns and examples.

### Adding New Features

To add a new feature (e.g., "Cancel Job"), you typically need:

1. **Domain Event** - Create event class in `services/__events/`
2. **API Endpoint** (if exposing HTTP endpoint) - Create handler in `services/<endpoint_name>/`
3. **Worker** (if processing events asynchronously) - Create handler in `services/<worker_name>/`
4. **Infrastructure** - Add Lambda, routes, rules, queues in `infra/`

For detailed step-by-step instructions, see the [Adding a New Feature](GUIDELINES.md#adding-a-new-feature) section in [GUIDELINES.md](GUIDELINES.md).

## Prerequisites

- **Python 3.12+** with pipenv
- **Node.js 18+** and npm (for AWS CDK)
- **AWS CLI** configured with appropriate credentials
- **AWS Account** with permissions to create:
  - DynamoDB tables
  - Lambda functions
  - API Gateway
  - EventBridge
  - SQS queues
  - IAM roles
  - CloudFormation stacks

## Getting Started

### 1. Clone and Setup Environment

```bash
git clone <repository-url>
cd python-ddb-event-driven

# Install Python dependencies
pipenv install --dev

# Activate virtual environment
pipenv shell
```

### 2. Configure AWS Credentials

```bash
# Configure AWS CLI (if not done already)
aws configure

# Verify access
aws sts get-caller-identity
```

### 3. Build & Deployment

```bash
# Package all Lambdas (builds and packages deployable Lambdas)
pipenv run build-dist

# Bootstrap CDK (one-time per account/region)
pipenv run bootstrap

# (Optional) Synthesize CloudFormation templates locally
pipenv run synth

# Deploy the stack
pipenv run deploy
```

### 4. Deployment Outputs

After successful deployment, you'll see outputs like:

```
Outputs:
pyDdbEd-dev.ApiEndpoint = https://example123.execute-api.us-east-1.amazonaws.com
pyDdbEd-dev.EventBusName = pyDdbEd-dev-event-driver-bus
pyDdbEd-dev.TableName = pyDdbEd-dev-ddb-event-store-table
```

## Testing the System

### 1. Create a Job (Trigger the Pipeline)

```bash
# Replace with your actual API endpoint from deployment outputs
export API_ENDPOINT="https://example123.execute-api.us-east-1.amazonaws.com"

# Create a new job
curl -X POST "${API_ENDPOINT}/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "JOB-12345",
    "job_name": "My Test Job"
  }'

# Expected response: 202 Accepted
```

### 2. List Job Events

```bash
# List all events for the job to see the progression
curl -X GET "${API_ENDPOINT}/jobs/JOB-12345/events"

# You should see all events in sequence:
# JOB_CREATED_EVENT → STEP_PROCESSED_EVENT →
# TASK_FOO_EXECUTED_EVENT, TASK_QUX_EXECUTED_EVENT, TASK_BAR_EXECUTED_EVENT →
# ALL_TASKS_COMPLETED_EVENT → JOB_FINALIZED_EVENT
```

### 3. Monitor Events in CloudWatch

```bash
# View logs for different components
aws logs tail /aws/lambda/pyDdbEd-dev-create-job-endpoint --follow
aws logs tail /aws/lambda/pyDdbEd-dev-process-step-worker --follow
aws logs tail /aws/lambda/pyDdbEd-dev-execute-task-foo-worker --follow
aws logs tail /aws/lambda/pyDdbEd-dev-execute-task-qux-worker --follow
aws logs tail /aws/lambda/pyDdbEd-dev-execute-task-bar-worker --follow
aws logs tail /aws/lambda/pyDdbEd-dev-complete-all-tasks-worker --follow
aws logs tail /aws/lambda/pyDdbEd-dev-finalize-job-worker --follow
```

### 4. Using REST Client

If you're using VS Code with REST Client extension:

1. Create a `.env` file: `BASE_URL=https://your-api-endpoint.amazonaws.com`
2. Open `_restclient/create-job-endpoint.http` or `_restclient/list-job-events-endpoint.http`
3. Click "Send Request" to test the endpoints

### 5. Testing Idempotency

```bash
# Send the same job creation request multiple times
# It should return 202 Accepted each time without creating duplicates
curl -X POST "${API_ENDPOINT}/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "JOB-12345",
    "job_name": "My Test Job"
  }'
```

## Teardown

### Complete Stack Removal

```bash
# Destroy all AWS resources
pipenv run destroy

# Confirm when prompted
```

**⚠️ Warning**: This will permanently delete:

- All DynamoDB data
- Lambda functions
- API Gateway
- EventBridge resources
- CloudWatch logs

### Partial Cleanup

```bash
# Only empty DynamoDB table (keep infrastructure)
aws dynamodb scan --table-name pyDdbEd-dev-ddb-event-store-table \
  --projection-expression "idempotencyKey" \
  --query "Items[].idempotencyKey.S" \
  --output text | xargs -I {} aws dynamodb delete-item \
  --table-name pyDdbEd-dev-ddb-event-store-table \
  --key '{"idempotencyKey":{"S":"{}"}}'
```

## Development

### Code Quality & Standards

```bash
# Format code
pipenv run format

# Lint and fix issues
pipenv run lint-fix

# Type checking
pipenv run type-check
```

### Adding New Event Types

1. **Define Event Classes**: Create new event in `services/__events/`

   ```python
   from pydantic import BaseModel
   from services.__events.event_base import EventBase

   class MyCustomEventData(BaseModel):
       job_id: str
       job_name: str
       job_status: str

   class MyCustomEvent(EventBase):
       eventName: str = "MY_CUSTOM_EVENT"
       eventData: MyCustomEventData
   ```

2. **Create Worker**: Add new worker in `services/my_custom_worker/`

3. **Update Infrastructure**: Add worker to `main_stack.py`

4. **Build & Deploy**: Run `pipenv run build-dist`

For detailed implementation guidelines, see [GUIDELINES.md](GUIDELINES.md).

### Local Development

```bash
# Install new dependencies
pipenv install package-name

# Generate requirements.txt for Lambda layers
pipenv run build-requirements

# Quick rebuild after code changes
pipenv run build-lambdas
pipenv run deploy
```

### Build System Conventions

Our build system follows two key conventions that automatically detect and package Lambda functions:

#### 1. Deployable Lambda Detection

A folder inside `services/` is considered a **"deployable Lambda"** if it contains a Python file that matches the folder's name.

**Examples:**

- `services/create_job_endpoint/create_job_endpoint.py` ✅ (deployable)
- `services/process_step_worker/process_step_worker.py` ✅ (deployable)
- `services/list_job_events_endpoint/list_job_events_endpoint.py` ✅ (deployable)
- `services/__events/event_base.py` ❌ (shared module, not deployable)

**Detection Logic**: Located in `deploy-scripts/build-all-lambdas.sh` and `deploy-scripts/build-all-requirements-txt.sh`

#### 2. Shared Module Convention

Any folder inside `services/` that starts with a **double underscore** (`__`) is considered a **"shared module"**.

**Shared modules:**

- `__events/` - Event classes and parsing logic
- `__errors/` - Custom exception hierarchy
- `__http_helpers/` - HTTP response utilities

#### 3. Build Process

When building a deployable Lambda, the build script (`_build_helpers.sh`) automatically:

1. **Detects** all deployable Lambdas using the naming convention
2. **Copies** all shared modules (`__*`) into each Lambda's build directory
3. **Installs** dependencies from each Lambda's individual `requirements.txt`
4. **Packages** everything into deployment-ready `.zip` files

**Trade-off**: Shared code gets duplicated across packages (not perfectly optimized), but this keeps the build process simple, reliable, and avoids complex dependency management at our current scale.

### Environment Variables

Set custom stack configuration:

```bash
export STACK_ID="my-project"
export STAGE_NAME="prod"
# Results in stack name: "my-project"
```

## Technical Details

### 🚀 Performance Characteristics

- **Cold Start**: ~200-500ms for Python Lambda functions
- **Throughput**: Limited by DynamoDB (40,000 RCU/WCU) and Lambda concurrency
- **Latency**: End-to-end job processing typically <10 seconds (depends on parallel task execution)

### 💰 Cost Considerations

- **DynamoDB**: Pay per request/storage
- **Lambda**: Pay per invocation/execution time
- **EventBridge**: $1/million events
- **API Gateway**: $3.50/million API calls
- **SQS**: $0.40 per million requests
- **Typical cost**: <$5/month for development workloads

### 🔧 Operational Features

- **CloudWatch Integration**: Automatic metrics and logging
- **X-Ray Tracing**: Enable by setting `TRACING_CONFIG` in Lambda constructs
- **Dead Letter Queues**: Failed events go to DLQ for investigation
- **Retry Logic**: Configurable retry attempts for worker functions (only transient errors are retried)

### 📈 Scaling Limits

- **DynamoDB**: Auto-scaling enabled, max 40K RCU/WCU per table
- **Lambda**: 1000 concurrent executions per region (can be increased)
- **EventBridge**: 10,000 rules per bus
- **SQS**: Virtually unlimited throughput

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes following the code style and [GUIDELINES.md](GUIDELINES.md)
4. Test thoroughly: `pipenv run type-check && pipenv run lint`
5. Commit: `git commit -m 'Add amazing feature'`
6. Push: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Built with ❤️ using AWS CDK, Python, and Serverless Technologies**
