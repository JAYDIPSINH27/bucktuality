import json
import os
import threading
import time
from datetime import datetime, timezone

from better_profanity import profanity
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
from fastapi import HTTPException

app = FastAPI(title="Bucktuality AI Moderation Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

SOURCE_TOPIC = "message-sent"
FLAGGED_TOPIC = "message-flagged"

flagged_messages = []
classifier = None


BAD_KEYWORDS = [
    "kill yourself",
    "i hate you",
    "stupid idiot",
    "die",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_model():
    global classifier

    while True:
        try:
            print("Loading AI moderation model...")

            classifier = pipeline(
                "text-classification",
                model="unitary/toxic-bert",
                top_k=None
            )

            print("AI moderation model loaded.")
            return

        except Exception as ex:
            print(f"Model loading failed: {ex}")
            time.sleep(10)


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
                group_id="ai-moderation-message-sent-group-v2",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            print("Connected to Kafka consumer.")
            return consumer

        except Exception as ex:
            print(f"Kafka consumer not ready: {ex}")
            time.sleep(5)


def analyze_with_rules(message: str):
    text = message.lower().strip()

    if not text:
        return None

    if profanity.contains_profanity(text):
        return {
            "isFlagged": True,
            "category": "profanity",
            "confidence": 0.85,
            "reason": "Profanity detected",
            "model": "rules"
        }

    for keyword in BAD_KEYWORDS:
        if keyword in text:
            return {
                "isFlagged": True,
                "category": "toxicity",
                "confidence": 0.90,
                "reason": f"Matched unsafe phrase: {keyword}",
                "model": "rules"
            }

    return None


def analyze_with_model(message: str):
    global classifier

    if classifier is None:
        return {
            "isFlagged": False,
            "category": "unknown",
            "confidence": 0.0,
            "reason": "Model not loaded",
            "model": "none"
        }

    results = classifier(message[:512])

    scores = results[0]

    highest = max(scores, key=lambda x: x["score"])

    label = highest["label"]
    confidence = float(highest["score"])

    if confidence >= 0.70:
        return {
            "isFlagged": True,
            "category": label.lower(),
            "confidence": confidence,
            "reason": f"AI model detected {label}",
            "model": "unitary/toxic-bert"
        }

    return {
        "isFlagged": False,
        "category": "safe",
        "confidence": confidence,
        "reason": "No serious issue detected",
        "model": "unitary/toxic-bert"
    }


def analyze_message(message: str):
    rule_result = analyze_with_rules(message)

    if rule_result is not None:
        return rule_result

    return analyze_with_model(message)


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
            "Model": result["model"],
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

    load_thread = threading.Thread(target=load_model, daemon=True)
    load_thread.start()

    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()


@app.get("/health")
def health():
    model_loaded = classifier is not None

    return {
        "service": "ai-moderation-service",
        "status": "healthy" if model_loaded else "starting",
        "modelLoaded": model_loaded,
    }


@app.get("/ready")
def ready():
    if classifier is None:
        raise HTTPException(
            status_code=503,
            detail="AI moderation model is still loading",
        )

    return {
        "service": "ai-moderation-service",
        "status": "ready",
        "modelLoaded": True,
    }

@app.get("/moderation/flagged")
def get_flagged_messages():
    return flagged_messages