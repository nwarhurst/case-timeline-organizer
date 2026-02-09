import json
import os
import boto3

s3 = boto3.client("s3")


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    bucket = os.environ["CASE_FILES_BUCKET"]

    path_params = event.get("pathParameters") or {}
    case_id = path_params.get("case_id")
    if not case_id:
        return _response(400, {"error": "Missing case_id in path."})

    prefix = f"cases/{case_id}/raw/"

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])

    files = []
    for obj in contents:
        key = obj["Key"]
        if key.endswith("/"):
            continue
        files.append(
            {
                "key": key,
                "size": obj.get("Size", 0),
                "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
            }
        )

    # Sort by last_modified then key for consistency
    files.sort(key=lambda x: (x["last_modified"] or "", x["key"]))

    return _response(200, {"case_id": case_id, "count": len(files), "files": files})
