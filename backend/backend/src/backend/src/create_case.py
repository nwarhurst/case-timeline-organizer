import json
import uuid
from datetime import datetime, timezone


def lambda_handler(event, context):
    case_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    body = {
        "case_id": case_id,
        "created_utc": now,
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
