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

    # case_id comes from the URL: /cases/{case_id}/presign
    path_params = event.get("pathParameters") or {}
    case_id = path_params.get("case_id")
    if not case_id:
        return _response(400, {"error": "Missing case_id in path."})

    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON body."})

    filename = payload.get("filename")
    content_type = payload.get("content_type", "application/octet-stream")

    if not filename or "/" in filename or "\\" in filename:
        return _response(400, {"error": "Invalid filename."})

    key = f"cases/{case_id}/raw/{filename}"

    url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ContentType": content_type,
        },
        ExpiresIn=900,  # 15 minutes
        HttpMethod="PUT",
    )

    return _response(
        200,
        {
            "upload_url": url,
            "s3_key": key,
            "expires_in_seconds": 900,
        },
    )
