
import base64
import binascii
import io
import json

from flask import Flask, request, jsonify
from fastavro import parse_schema, schemaless_reader

from scheduler.scheduled_processor import run_hourly_processing
from common.logger import get_logger
from common.order_processor import process_order
from common.database import (
    claim_order,
    mark_order_processed,
    release_order
)

PROJECT_ID = "pioneering-flow-436606-t9"
SCHEMA_PATH = "schemas/order.avsc"

app = Flask(__name__)

logger = get_logger("cloud-run-order-processor")


# Load Avro schema when the application starts
def load_avro_schema():
    with open(SCHEMA_PATH, "r") as schema_file:
        schema = json.load(schema_file)

    return parse_schema(schema)


AVRO_SCHEMA = load_avro_schema()


# Deserialize Avro binary data
def deserialize_avro(message_data):
    buffer = io.BytesIO(message_data)

    return schemaless_reader(
        buffer,
        AVRO_SCHEMA
    )


# Process an incoming Pub/Sub message
def handle_order(message_data, message_type):

    order_id = None
    lease_token = None

    try:
        if message_type == "JSON":
            order = json.loads(
                message_data.decode("utf-8")
            )

        elif message_type == "AVRO":
            order = deserialize_avro(message_data)

        else:
            logger.warning(
                "Unsupported message type: %s",
                message_type
            )
            return "SKIPPED"

        if not isinstance(order, dict):
            raise ValueError("Order payload must be an object")

        order_id = order.get("order_id")

        if not order_id:
            raise ValueError("Missing order_id")

        logger.info(
            "Received %s order: %s",
            message_type,
            order_id
        )

        # Reserve order in Firestore
        status, lease_token = claim_order(order_id)

        if status == "DUPLICATE":
            logger.warning(
                "Duplicate order skipped: %s",
                order_id
            )
            return "DUPLICATE"

        if status == "IN_PROGRESS":
            logger.warning(
                "Order already being processed: %s",
                order_id
            )
            return "IN_PROGRESS"

        # Validate and transform order
        processed_order = process_order(order)

        # Store successful processing result
        mark_order_processed(
            order_id,
            lease_token,
            processed_order
        )

        logger.info(
            "Order successfully processed: %s",
            order_id
        )

        return "PROCESSED"

    except Exception:
        logger.exception(
            "Order processing failed: %s",
            order_id
        )

        if order_id and lease_token:
            try:
                release_order(
                    order_id,
                    lease_token
                )
            except Exception:
                logger.exception(
                    "Failed to release order reservation: %s",
                    order_id
                )

        raise


# Health check endpoint
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "service": "ecommerce-order-processor",
        "status": "healthy"
    }), 200


# Pub/Sub push endpoint
@app.route("/", methods=["POST"])
def receive_pubsub_message():

    envelope = request.get_json(silent=True)

    if not isinstance(envelope, dict):
        logger.error("Invalid Pub/Sub request envelope")
        return jsonify({"error": "Invalid envelope"}), 400

    message = envelope.get("message")

    if not isinstance(message, dict):
        logger.error("Pub/Sub message is missing")
        return jsonify({"error": "Missing message"}), 400

    attributes = message.get("attributes", {})

    if not isinstance(attributes, dict):
        logger.error("Invalid message attributes")
        return jsonify({"error": "Invalid attributes"}), 400

    message_type = attributes.get("message_type")

    if message_type not in ("JSON", "AVRO"):
        logger.error(
            "Missing or unsupported message_type: %s",
            message_type
        )
        return "", 204

    encoded_data = message.get("data")

    if not isinstance(encoded_data, str):
        logger.error("Message data is missing")
        return "", 204

    try:
        message_data = base64.b64decode(
            encoded_data,
            validate=True
        )

    except (binascii.Error, ValueError):
        logger.exception("Invalid Base64 message data")
        return "", 204

    try:
        result = handle_order(
            message_data,
            message_type
        )

        if result == "IN_PROGRESS":
            return jsonify({
                "status": "order_in_progress"
            }), 503

        return "", 204

    except Exception:
        return jsonify({
            "status": "processing_failed"
        }), 500


# Cloud Scheduler endpoint
@app.route("/scheduled", methods=["POST"])
def scheduled_processing():

    try:
        summary = run_hourly_processing()

        return jsonify({
            "status": "success",
            "message": "Hourly order summary generated",
            "total_orders": summary["total_orders"],
            "total_sales": summary["total_sales"],
            "summary_id": summary["summary_id"]
        }), 200

    except Exception:
        logger.exception(
            "Hourly scheduled processing failed"
        )

        return jsonify({
            "status": "failed"
        }), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=8080
    )
