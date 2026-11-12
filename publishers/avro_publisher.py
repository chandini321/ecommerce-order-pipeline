
import sys
import time
import io
import json

from fastavro import parse_schema, schemaless_writer
from google.cloud import pubsub_v1

from common.logger import get_logger


PROJECT_ID = "pioneering-flow-436606-t9"
TOPIC_ID = "orders-topic-practice"

SCHEMA_PATH = "schemas/order.avsc"

logger = get_logger("avro-publisher")


def load_avro_schema():

    with open(SCHEMA_PATH, "r") as schema_file:
        schema = json.load(schema_file)

    return parse_schema(schema)


def serialize_order(order, schema):

    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        order
    )

    return buffer.getvalue()


def publish_orders():

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(
        PROJECT_ID,
        TOPIC_ID
    )

    publisher_name = (
        sys.argv[1] if len(sys.argv) > 1 else "AVRO-Publisher-1"
    )

    schema = load_avro_schema()

    orders = [
        {
            "order_id": f"{publisher_name}_ORD2001",
            "customer_id": "CUST201",
            "product": "Laptop",
            "quantity": 1,
            "price": 55000.0
        },
        {
            "order_id": f"{publisher_name}_ORD2002",
            "customer_id": "CUST202",
            "product": "Headphones",
            "quantity": 2,
            "price": 2500.0
        },
        {
            "order_id": f"{publisher_name}_ORD2003",
            "customer_id": "CUST203",
            "product": "Smartphone",
            "quantity": 1,
            "price": 30000.0
        },
        {
            "order_id": f"{publisher_name}_ORD2004",
            "customer_id": "CUST204",
            "product": "Keyboard",
            "quantity": 1,
            "price": 1500.0
        },
        {
            "order_id": f"{publisher_name}_ORD2005",
            "customer_id": "CUST205",
            "product": "Mouse",
            "quantity": 2,
            "price": 800.0
        }
    ]

    logger.info(
        "Starting Avro publishing. Publisher: %s",
        publisher_name
    )

    try:

        for order in orders:

            # Convert order into Avro binary
            message_data = serialize_order(
                order,
                schema
            )

            future = publisher.publish(
                topic_path,
                message_data,
                message_type="AVRO",
                order_id=order["order_id"]
            )

            message_id = future.result()

            logger.info(
                "Published Avro order_id=%s message_id=%s",
                order["order_id"],
                message_id
            )

            time.sleep(1)

        logger.info(
            "Avro publishing completed. Total orders: %s",
            len(orders)
        )

    except Exception:

        logger.exception("Avro publishing failed")
        raise

    finally:
        publisher.transport.close()


if __name__ == "__main__":
    publish_orders()
