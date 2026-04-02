from fnmatch import fnmatch
from typing import Any, Dict, List, Optional

import boto3


s3_client = boto3.client("s3")


def detect_file_type(object_key: str, required_files: List[Dict[str, Any]]) -> Optional[str]:
    file_name = object_key.split("/")[-1].lower()
    for required in required_files:
        for pattern in required.get("patterns", []):
            if fnmatch(file_name, pattern.lower()):
                return required["file_type"]
    return None


def _list_client_objects(bucket: str, client_id: str) -> List[Dict[str, Any]]:
    prefix = f"raw/client_uploads/{client_id}/"
    results: List[Dict[str, Any]] = []
    continuation = None
    while True:
        args: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            args["ContinuationToken"] = continuation
        response = s3_client.list_objects_v2(**args)
        results.extend(response.get("Contents", []))
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
    return results


def find_latest_required_files(bucket: str, client_id: str, required_files: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    objects = _list_client_objects(bucket, client_id)
    by_file_type: Dict[str, Dict[str, Any]] = {}

    for obj in objects:
        key = obj["Key"]
        file_type = detect_file_type(key, required_files)
        if not file_type:
            continue

        previous = by_file_type.get(file_type)
        previous_last_modified = previous["_last_modified"] if previous else None
        if not previous or obj["LastModified"] > previous_last_modified:
            by_file_type[file_type] = {
                "key": key,
                "etag": obj.get("ETag", "").strip("\""),
                "size": obj.get("Size", 0),
                "last_modified": obj["LastModified"].isoformat(),
                "_last_modified": obj["LastModified"]
            }

    return {
        file_type: {k: v for k, v in payload.items() if k != "_last_modified"}
        for file_type, payload in by_file_type.items()
    }
