import json
from datetime import datetime, timezone
from typing import Any, Dict

import boto3


class StepFunctionStarter:
    def __init__(self, state_machine_arn: str) -> None:
        self._state_machine_arn = state_machine_arn
        self._client = boto3.client("stepfunctions")

    def start(self, client_id: str, input_payload: Dict[str, Any]) -> str:
        execution_name = f"{client_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        response = self._client.start_execution(
            stateMachineArn=self._state_machine_arn,
            name=execution_name[:80],
            input=json.dumps(input_payload)
        )
        return response["executionArn"]
