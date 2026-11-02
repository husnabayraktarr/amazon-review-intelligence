"""
Real-time review quality alerts.
Kinesis -> Lambda -> DynamoDB (hourly counts per product, exactly-once) -> SNS alert (once per product-hour)
Invalid records go to an SQS queue instead of failing the batch.
"""
import base64
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

ddb = boto3.client("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")

TABLE = os.environ["TABLE_NAME"]
TOPIC_ARN = os.environ["TOPIC_ARN"]
INVALID_QUEUE_URL = os.environ["INVALID_QUEUE_URL"]
MIN_REVIEWS = int(os.environ.get("MIN_REVIEWS", "10"))
ONE_STAR_RATE = float(os.environ.get("ONE_STAR_RATE", "0.5"))
TTL_SECONDS = 7 * 24 * 3600


def parse(record):
    """Decode and validate one Kinesis record. Raises ValueError with a reason if invalid."""
    e = json.loads(base64.b64decode(record["kinesis"]["data"]))
    if not isinstance(e.get("event_id"), str) or not e["event_id"]:
        raise ValueError("missing event_id")
    if not isinstance(e.get("parent_asin"), str) or not e["parent_asin"]:
        raise ValueError("missing parent_asin")
    if not isinstance(e.get("rating"), int) or not 1 <= e["rating"] <= 5:
        raise ValueError(f"bad rating: {e.get('rating')!r}")
    try:
        e["ts"] = datetime.fromisoformat(e["event_ts"]).astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError):
        raise ValueError(f"bad event_ts: {e.get('event_ts')!r}")
    return e


def record_review(e, expires):
    """Exactly-once counting: mark the event as seen AND update the hourly counts in one transaction.
    Returns (window, total, one_star) or None if this event was already counted."""
    window = e["ts"].strftime("%Y-%m-%dT%H:00Z")
    key = {"pk": {"S": f"PRODUCT#{e['parent_asin']}"}, "sk": {"S": f"WINDOW#{window}"}}
    try:
        ddb.transact_write_items(TransactItems=[
            {"Put": {
                "TableName": TABLE,
                "Item": {"pk": {"S": f"EVENT#{e['event_id']}"}, "sk": {"S": "SEEN"},
                         "expires_at": {"N": str(expires)}},
                "ConditionExpression": "attribute_not_exists(pk)",
            }},
            {"Update": {
                "TableName": TABLE,
                "Key": key,
                "UpdateExpression": "ADD review_count :one, one_star_count :star SET expires_at = :exp",
                "ExpressionAttributeValues": {
                    ":one": {"N": "1"},
                    ":star": {"N": "1" if e["rating"] == 1 else "0"},
                    ":exp": {"N": str(expires)},
                },
            }},
        ])
    except ClientError as err:
        reasons = err.response.get("CancellationReasons", [])
        if err.response["Error"]["Code"] == "TransactionCanceledException" and reasons \
                and reasons[0].get("Code") == "ConditionalCheckFailed":
            return None  # duplicate delivery from Kinesis: already counted
        raise
    item = ddb.get_item(TableName=TABLE, Key=key, ConsistentRead=True)["Item"]
    return window, int(item["review_count"]["N"]), int(item["one_star_count"]["N"])


def alert_once(asin, window, total, ones):
    """Conditional write guarantees one alert per product-hour, even with retries or parallel batches."""
    key = {"pk": {"S": f"PRODUCT#{asin}"}, "sk": {"S": f"WINDOW#{window}"}}
    try:
        ddb.update_item(
            TableName=TABLE, Key=key,
            UpdateExpression="SET alerted_at = :now",
            ConditionExpression="attribute_not_exists(alerted_at)",
            ExpressionAttributeValues={":now": {"S": datetime.now(timezone.utc).isoformat()}},
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=f"Quality alert: product {asin}"[:100],
        Message=(
            f"Product {asin} is getting a burst of 1-star reviews.\n\n"
            f"Hour window: {window}\n"
            f"Reviews this hour: {total}\n"
            f"1-star reviews: {ones} ({ones / total:.0%})\n"
            f"Threshold: at least {MIN_REVIEWS} reviews and {ONE_STAR_RATE:.0%} 1-star\n\n"
            f"Product page: https://www.amazon.com/dp/{asin}\n"
        ),
    )
    return True


def handler(event, context):
    failures = []
    stats = {"counted": 0, "duplicates": 0, "invalid": 0, "alerts": 0}
    expires = int(time.time()) + TTL_SECONDS

    for record in event["Records"]:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            e = parse(record)
        except (ValueError, KeyError, json.JSONDecodeError) as err:
            raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8", "replace")
            sqs.send_message(QueueUrl=INVALID_QUEUE_URL,
                             MessageBody=json.dumps({"reason": str(err), "record": raw}))
            stats["invalid"] += 1
            continue

        try:
            result = record_review(e, expires)
            if result is None:
                stats["duplicates"] += 1
                continue
            window, total, ones = result
            stats["counted"] += 1
            if total >= MIN_REVIEWS and ones / total >= ONE_STAR_RATE and \
                    alert_once(e["parent_asin"], window, total, ones):
                stats["alerts"] += 1
        except Exception as err:  # unexpected (throttling, network): retry just this record
            print(f"[error] seq={seq} error={err}")
            failures.append({"itemIdentifier": seq})

    print(f"[batch] records={len(event['Records'])} {json.dumps(stats)} failed={len(failures)}")
    return {"batchItemFailures": failures}
