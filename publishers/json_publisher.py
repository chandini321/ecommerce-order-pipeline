
import json
import sys
import time

from google.cloud import pubsub_v1

from common.logger import get_logger


PROJECT_ID = "pioneering-flow-436606-t9"
TOPIC_ID = "orders-topic-practice"

logger = get_logger("json-publisher")


def publish_orders():

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(
        PROJECT_ID,
        TOPIC_ID
    )

    publisher_name = (
        sys.argv[1] if len(sys.argv) > 1 else "JSON-Publisher-1"
    )

    orders = [
        {
            "order_id": f"{publisher_name}_ORD1001",
            "customer_id": "CUST101",
            "product": "Laptop",
            "quantity": 1,
            "price": 55000
        },
        {
            "order_id": f"{publisher_name}_ORD1002",
            "customer_id": "CUST102",
            "product": "Headphones",
            "quantity": 2,
            "price": 2500
        },
        {
            "order_id": f"{publisher_name}_ORD1003",
            "customer_id": "CUST103",
            "product": "Smartphone",
            "quantity": 1,
            "price": 30000
        },
        {
            "order_id": f"{publisher_name}_ORD1004",
            "customer_id": "CUST104",
            "product": "Keyboard",
            "quantity": 1,
            "price": 1500
        },
        {
            "order_id": f"{publisher_name}_ORD1005",
            "customer_id": "CUST105",
            "product": "Mouse",
            "quantity": 2,
            "price": 800
        }
    ]

    logger.info(
        "Starting JSON publishing. Publisher: %s",
        publisher_name
    )

    try:

        for order in orders:

            message_data = json.dumps(order).encode("utf-8")

            future = publisher.publish(
                topic_path,
                message_data,
                message_type="JSON",
                order_id=order["order_id"]
            )

            message_id = future.result()

            logger.info(
                "Published order_id=%s message_id=%s",
                order["order_id"],
                message_id
            )

            time.sleep(1)

        logger.info(
            "JSON publishing completed. Total orders: %s",
            len(orders)
        )

    except Exception:

        logger.exception("JSON publishing failed")
        raise

    finally:
        publisher.transport.close()


if __name__ == "__main__":
    publish_orders()
