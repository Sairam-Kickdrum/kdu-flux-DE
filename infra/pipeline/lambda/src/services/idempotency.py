import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError


class IdempotencyStore:
    def __init__(self, table_name: str) -> None:
        self._table = boto3.resource("dynamodb").Table(table_name)

    def put_if_absent(self, key: str, payload: Dict[str, Any], ttl_days: int = 30) -> bool:
        ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=ttl_days)).timestamp())
        item = {
            "key": key,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "ttl": ttl_epoch,
            "payload": json.dumps(payload)
        }
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(#k)",
                ExpressionAttributeNames={"#k": "key"}
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
