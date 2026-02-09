import csv
import io
import json
import os
import boto3
from datetime import datetime, timezone
import re

s3 = boto3.client("s3")

DATE_PATTERNS = [
    # 2026-02-08 or 2026_02_08
    re.compile(r"(?P<y>20\d{2})[-_](?P<m>\d{2})[-_](?P<d>\d{2})"),
    # 02-08-2026 or 02_08_2026
    re.compile(r"(?P<m>\d{2})[-_](?P<d>\d{2})[-_](?P<y>20\d{2})"),
]


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def _parse_date_from_filename(name: str):
    for pat in DATE_PATTERNS:
        m = pat.search(name)
        if not m:
            continue
        try:
            y = int(m.group("y"))
            mo = int(m.group("m"))
            d = int(m.group("d"))
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def lambda_handler(event, context):
    bucket = os.environ["CASE_FILES_BUCKET"]

    path_params = event.get("pathParameters") or {}
    case_id = path_params.get("case_id")
    if not case_id:
        return _response(400, {"error": "Missing case_id in path."})

    raw_prefix = f"cases/{case_id}/raw/"
    out_key = f"cases/{case_id}/outputs/index.csv"

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
    contents = resp.get("Contents", [])

    rows = []
    for obj in contents:
        key = obj["Key"]
        if key.endswith("/"):
            continue

        filename = key.split("/")[-1]
        parsed = _parse_date_from_filename(filename)
        lm = obj.get("LastModified")
        best_dt = parsed or (lm.replace(tzinfo=timezone.utc) if lm else None)

        rows.append(
            {
                "best_date_utc": best_dt.isoformat() if best_dt else "",
                "source": "filename" if parsed else "s3_last_modified",
                "s3_key": key,
                "filename": filename,
                "size_bytes": obj.get("Size", 0),
            }
        )

    rows.sort(key=lambda r: (r["best_date_utc"], r["filename"]))

    csv_buf = io.StringIO()
    writer = csv.DictWriter(
        csv_buf,
        fieldnames=["best_date_utc", "source", "filename", "size_bytes", "s3_key"],
    )
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=csv_buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    download_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": out_key},
        ExpiresIn=900,
    )

    return _response(
        200,
        {
            "case_id": case_id,
            "count": len(rows),
            "index_key": out_key,
            "download_url": download_url,
            "expires_in_seconds": 900,
        },
    )
