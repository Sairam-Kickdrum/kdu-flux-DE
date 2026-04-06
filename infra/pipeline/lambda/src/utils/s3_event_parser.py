import json
from typing import Any, Dict, List
from urllib.parse import unquote_plus


def _decode_s3_key(record: Dict[str, Any]) -> None:
    key = record.get("s3", {}).get("object", {}).get("key")
    if key:
        record["s3"]["object"]["key"] = unquote_plus(key)


def extract_s3_records_from_sqs_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for sqs_record in event.get("Records", []):
        body = sqs_record.get("body", "{}")
        payload = json.loads(body)

        # Support both direct S3 event in SQS and SNS-wrapped payloads.
        if "Message" in payload:
            payload = json.loads(payload["Message"])

        for s3_record in payload.get("Records", []):
            if s3_record.get("eventSource") == "aws:s3":
                _decode_s3_key(s3_record)
                extracted.append(s3_record)
    return extracted
