
import json

from google.cloud import pubsub_v1

from common.logger import get_logger
from common.order_processor import process_order
from common.database import (
    claim_order,
    mark_order_processed,
    release_order
)


PROJECT_ID = "pioneering-flow-436606-t9"
SUBSCRIPTION_ID = "orders-sub-json"

logger = get_logger("json-subscriber")


def callback(message):

    message_type = message.attributes.get("message_type")

    # Ignore messages belonging to Avro subscriber
    if message_type != "JSON":

        logger.info(
            "Skipping non-JSON message. Message ID: %s",
            message.message_id
        )

        message.ack()
        return

    lease_token = None
    order_id = None

    try:

        # Deserialize JSON message
        order = json.loads(
            message.data.decode("utf-8")
        )

        order_id = order.get("order_id")

        if not order_id:
            raise ValueError("Missing order_id")

        logger.info(
            "Received JSON order: %s",
            order_id
        )

        # Check duplicate and claim order
        status, lease_token = claim_order(order_id)

        if status == "DUPLICATE":

            logger.warning(
                "Duplicate order detected: %s",
                order_id
            )

            message.ack()
            return

        if status == "IN_PROGRESS":

            logger.info(
                "Order is already being processed: %s",
                order_id
            )

            message.nack()
            return

        # Validate and transform order
        processed_order = process_order(order)

        # Mark successfully processed in Firestore
        mark_order_processed(
            order_id,
            lease_token,
            processed_order
        )

        logger.info(
            "JSON order processed successfully: %s",
            order_id
        )

        message.ack()

    except Exception:

        logger.exception(
            "JSON order processing failed: %s",
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
                    "Failed to release order: %s",
                    order_id
                )

        message.nack()


def main():

    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(
        PROJECT_ID,
        SUBSCRIPTION_ID
    )

    logger.info(
        "JSON subscriber started. Subscription: %s",
        SUBSCRIPTION_ID
    )

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback
    )

    try:
        streaming_pull_future.result()

    except KeyboardInterrupt:
        streaming_pull_future.cancel()

    finally:
        subscriber.close()

        logger.info("JSON subscriber stopped.")


if __name__ == "__main__":
    main()
