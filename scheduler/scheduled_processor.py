
from datetime import datetime, timedelta, timezone

from google.cloud import firestore

from common.logger import get_logger


PROJECT_ID = "pioneering-flow-436606-t9"

ORDERS_COLLECTION = "processed_orders"
SUMMARY_COLLECTION = "hourly_order_summaries"

logger = get_logger("scheduled-order-processor")

db = firestore.Client(project=PROJECT_ID)


def run_hourly_processing():

    now = datetime.now(timezone.utc)

    hour_start = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    hour_end = hour_start + timedelta(hours=1)

    logger.info(
        "Starting hourly order summary: %s",
        hour_start.isoformat()
    )

    # Fetch successfully processed orders
    orders_query = (
        db.collection(ORDERS_COLLECTION)
        .where("status", "==", "PROCESSED")
        .where("processed_at", ">=", hour_start)
        .where("processed_at", "<", hour_end)
    )

    orders = list(orders_query.stream())

    total_orders = 0
    total_sales = 0.0

    for document in orders:

        order_data = document.to_dict()

        processed_order = order_data.get(
            "processed_order",
            {}
        )

        total_orders += 1

        total_sales += float(
            processed_order.get("total_amount", 0)
        )

    # Create a deterministic document ID for this hour
    summary_id = hour_start.strftime("%Y%m%d%H")

    summary = {
        "summary_id": summary_id,
        "total_orders": total_orders,
        "total_sales": round(total_sales, 2),
        "period_start": hour_start,
        "period_end": hour_end,
        "generated_at": now,
        "status": "COMPLETED"
    }

    # Save or update the hourly summary
    db.collection(SUMMARY_COLLECTION).document(
        summary_id
    ).set(summary)

    logger.info(
        "Hourly summary completed | Orders: %s | Sales: %s",
        total_orders,
        round(total_sales, 2)
    )

    return summary
