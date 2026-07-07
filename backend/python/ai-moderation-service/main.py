import json
import os
import threading
import time
from datetime import datetime, timezone

from better_profanity import profanity
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer


app = FastAPI(title="Bucktuality AI Moderation Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

FLAGGED_TOPIC = "message-flagged"
SOURCE_TOPIC = "message-sent"

flagged_messages = []


BAD_KEYWORDS = [
    "kill yourself",
    "i hate you",
    "stupid idiot",
    "die",
    "terrorist",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka producer.")
            return producer
        except Exception as ex:
            print(f"Kafka producer not ready: {ex}")
            time.sleep(5)


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                SOURCE_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id="ai-moderation-message-sent-group-v1",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print("Connected to Kafka consumer.")
            return consumer
        except Exception as ex:
            print(f"Kafka consumer not ready: {ex}")
            time.sleep(5)


def analyze_message(message: str):
    text = message.lower().strip()

    if not text:
        return {
            "isFlagged": False,
            "category": "none",
            "confidence": 0.0,
            "reason": "Empty message",
        }

    if profanity.contains_profanity(text):
        return {
            "isFlagged": True,
            "category": "profanity",
            "confidence": 0.85,
            "reason": "Profanity detected",
        }

    for keyword in BAD_KEYWORDS:
        if keyword in text:
            return {
                "isFlagged": True,
                "category": "toxicity",
                "confidence": 0.90,
                "reason": f"Matched unsafe phrase: {keyword}",
            }

    if len(text) > 300:
        return {
            "isFlagged": True,
            "category": "spam",
            "confidence": 0.75,
            "reason": "Message too long",
        }

    return {
        "isFlagged": False,
        "category": "safe",
        "confidence": 0.10,
        "reason": "No moderation issue detected",
    }


def consume_messages():
    producer = create_producer()
    consumer = create_consumer()

    for record in consumer:
        event = record.value

        print("Received message-sent event:", event)

        message_text = event.get("Message") or event.get("message") or ""

        result = analyze_message(message_text)

        if not result["isFlagged"]:
            continue

        flagged_event = {
            "EventType": "MessageFlagged",
            "RoomId": event.get("RoomId") or event.get("roomId") or "",
            "SenderUserId": event.get("SenderUserId") or event.get("senderUserId") or "",
            "Message": message_text,
            "Category": result["category"],
            "Confidence": result["confidence"],
            "Reason": result["reason"],
            "CreatedAtUtc": utc_now(),
        }

        flagged_messages.insert(0, flagged_event)

        if len(flagged_messages) > 100:
            flagged_messages.pop()

        producer.send(FLAGGED_TOPIC, flagged_event)
        producer.flush()

        print("Published message-flagged event:", flagged_event)


@app.on_event("startup")
def startup_event():
    profanity.load_censor_words()

    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {
        "service": "ai-moderation-service",
        "status": "healthy",
    }


@app.get("/moderation/flagged")
def get_flagged_messages():
    return flagged_messages