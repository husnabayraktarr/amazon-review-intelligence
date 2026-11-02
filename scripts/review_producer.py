"""
Live review producer: replays real reviews into Kinesis as if customers were writing them right now.

Usage:
  python scripts/review_producer.py --file data/stream_sample.csv --rate 20 --limit 600
  python scripts/review_producer.py --file data/stream_sample.csv --incident B07YF9SGBW --incident-count 40
  python scripts/review_producer.py --file data/stream_sample.csv --malformed 5
"""
import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

STREAM = "review-events"
kinesis = boto3.client("kinesis", region_name="us-east-1")


def to_event(row, parent_asin=None, rating=None):
    return {
        "event_id": str(uuid.uuid4()),                      # unique per event, used for idempotency later
        "event_ts": datetime.now(timezone.utc).isoformat(),  # "now": these are live reviews
        "source_review_id": row["review_id"],
        "parent_asin": parent_asin or row["parent_asin"],
        "user_key": row["user_key"],
        "rating": int(rating if rating is not None else row["rating"]),
        "title": row["title"],
        "text": row["text"],
        "helpful_vote": int(row["helpful_vote"] or 0),
        "verified_purchase": row["verified_purchase"].lower() == "true",
        "has_images": row["has_images"].lower() == "true",
    }


def send(events):
    """PutRecords with retries for partially failed batches. Partition key = product, so one product's
    reviews stay in order on one shard."""
    records = [{"Data": (json.dumps(e) + "\n").encode("utf-8"), "PartitionKey": e["parent_asin"]}
               for e in events]
    for attempt in range(5):
        resp = kinesis.put_records(StreamName=STREAM, Records=records)
        if resp["FailedRecordCount"] == 0:
            return 0
        records = [rec for rec, res in zip(records, resp["Records"]) if "ErrorCode" in res]
        time.sleep(0.2 * 2 ** attempt)
    return len(records)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--rate", type=int, default=20, help="events per second")
    ap.add_argument("--limit", type=int, default=600, help="normal events to send")
    ap.add_argument("--incident", help="parent_asin to hit with a burst of 1-star reviews")
    ap.add_argument("--incident-count", type=int, default=40)
    ap.add_argument("--malformed", type=int, default=0, help="send broken events to test validation")
    args = ap.parse_args()

    with open(args.file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    sent = dropped = 0
    if args.malformed:
        bad = [{"event_id": str(uuid.uuid4()), "event_ts": datetime.now(timezone.utc).isoformat(),
                "parent_asin": "BROKEN", "rating": "not-a-number"} for _ in range(args.malformed)]
        records = [{"Data": (json.dumps(e) + "\n").encode(), "PartitionKey": "BROKEN"} for e in bad]
        kinesis.put_records(StreamName=STREAM, Records=records)
        print(f"[producer] sent {args.malformed} malformed events")
        return

    if args.incident:
        # A bad batch ships: about 90% of new reviews for one product are 1-star
        templates = random.sample(rows, args.incident_count)
        events = [to_event(r, parent_asin=args.incident, rating=1 if random.random() < 0.9 else 3)
                  for r in templates]
        for i in range(0, len(events), 10):
            dropped += send(events[i:i + 10])
            sent += len(events[i:i + 10])
            time.sleep(1)
        print(f"[producer] incident on {args.incident}: sent={sent} dropped={dropped}")
        return

    for i in range(0, min(args.limit, len(rows)), args.rate):
        batch = [to_event(r) for r in rows[i:i + args.rate]]
        dropped += send(batch)
        sent += len(batch)
        if sent % 200 == 0:
            print(f"[producer] sent={sent}")
        time.sleep(1)
    print(f"[producer] done: sent={sent} dropped={dropped}")


if __name__ == "__main__":
    main()
