from aws_cdk import RemovalPolicy, aws_dynamodb
from constructs import Construct


class DynamoDbConstruct(Construct):
    """
    Creates a DynamoDB table with NEW_IMAGE stream.
    Exposes: self.table, self.table_stream_arn
    """

    table: aws_dynamodb.Table
    table_stream_arn: str

    def __init__(self, scope: Construct, construct_id: str, *, base_name: str) -> None:
        super().__init__(scope, construct_id)

        # --- naming ---
        table_name = f"{base_name}-table"

        # --- resources ---
        self.table = aws_dynamodb.Table(
            self,
            "Table",
            table_name=table_name,
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE,
        )

        self.table_stream_arn = self.table.table_stream_arn if self.table.table_stream_arn else ""
