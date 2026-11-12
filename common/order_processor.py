
from datetime import datetime, timezone


REQUIRED_FIELDS = [
    "order_id",
    "customer_id",
    "product",
    "quantity",
    "price"
]


def validate_order(order):
    """
    Validate the incoming order.
    Returns (True, None) if valid,
    otherwise (False, error_message).
    """

    if not isinstance(order, dict):
        return False, "Order must be a dictionary"

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in order:
            return False, f"Missing required field: {field}"

    # Check string fields
    for field in ["order_id", "customer_id", "product"]:
        value = order[field]

        if not isinstance(value, str) or not value.strip():
            return False, f"{field} must be a non-empty string"

    # Check quantity
    quantity = order["quantity"]

    if isinstance(quantity, bool) or not isinstance(quantity, int):
        return False, "Quantity must be an integer"

    if quantity <= 0:
        return False, "Quantity must be greater than zero"

    # Check price
    price = order["price"]

    if isinstance(price, bool) or not isinstance(price, (int, float)):
        return False, "Price must be numeric"

    if price <= 0:
        return False, "Price must be greater than zero"

    return True, None


def process_order(order):
    """
    Validate and transform an order.
    Returns the processed order dictionary.
    """

    is_valid, error = validate_order(order)

    if not is_valid:
        raise ValueError(error)

    quantity = order["quantity"]
    price = float(order["price"])

    total_amount = quantity * price

    processed_order = {
        "order_id": order["order_id"].strip(),
        "customer_id": order["customer_id"].strip(),
        "product": order["product"].strip(),
        "quantity": quantity,
        "price": price,
        "total_amount": round(total_amount, 2),
        "status": "PROCESSED",
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    return processed_order
