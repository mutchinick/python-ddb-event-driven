import os

import aws_cdk as cdk
from main_stack import MainStack

stack_name = os.getenv("STACK_ID", "pyDdbEd")
stage_name = os.getenv("STAGE_NAME", "dev")
stack_id = f"{stack_name}-{stage_name}"

app = cdk.App()
MainStack(app, stack_id)


app.synth()
