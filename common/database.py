
import hashlib
import uuid

from datetime import datetime, timedelta, timezone

from google.cloud import firestore


PROJECT_ID = "pioneering-flow-436606-t9"

COLLECTION_NAME = "processed_orders"

LEASE_DURATION_MINUTES = 5

db = firestore.Client(project=PROJECT_ID)


def get_document_id(order_id):
    """
    Generate a safe Firestore document ID
    from the order ID.
    """

    return hashlib.sha256(
        order_id.encode("utf-8")
    ).hexdigest()


@firestore.transactional
def _claim_order_transaction(
    transaction,
    document_ref,
    order_id,
    lease_token
):
    """
    Atomically check and reserve an order.
    """

    snapshot = document_ref.get(
        transaction=transaction
    )

    now = datetime.now(timezone.utc)

    if snapshot.exists:

        existing_data = snapshot.to_dict()

        status = existing_data.get("status")

        # Already successfully processed
        if status == "PROCESSED":
            return "DUPLICATE"

        # Another consumer has an active processing lease
        if status == "PROCESSING":

            lease_expiry = existing_data.get(
                "lease_expiry"
            )

            if lease_expiry and lease_expiry > now:
                return "IN_PROGRESS"

        # Failed or expired processing can be retried

    transaction.set(
        document_ref,
        {
            "order_id": order_id,
            "status": "PROCESSING",
            "lease_token": lease_token,
            "lease_expiry": now + timedelta(
                minutes=LEASE_DURATION_MINUTES
            ),
            "updated_at": now
        }
    )

    return "CLAIMED"


def claim_order(order_id):
    """
    Reserve an order before processing.

    Returns:
        status, lease_token
    """

    document_id = get_document_id(order_id)

    document_ref = (
        db.collection(COLLECTION_NAME)
        .document(document_id)
    )

    transaction = db.transaction()

    lease_token = str(uuid.uuid4())

    status = _claim_order_transaction(
        transaction,
        document_ref,
        order_id,
        lease_token
    )

    return status, lease_token


@firestore.transactional
def _complete_order_transaction(
    transaction,
    document_ref,
    lease_token,
    processed_order
):
    """
    Mark an order as processed only if
    the current consumer owns its lease.
    """

    snapshot = document_ref.get(
        transaction=transaction
    )

    if not snapshot.exists:
        raise RuntimeError(
            "Order reservation does not exist"
        )

    data = snapshot.to_dict()

    if data.get("lease_token") != lease_token:
        raise RuntimeError(
            "Order processing lease is no longer owned"
        )

    transaction.update(
        document_ref,
        {
            "status": "PROCESSED",
            "processed_order": processed_order,
            "processed_at": datetime.now(timezone.utc),
            "lease_expiry": None,
            "lease_token": None
        }
    )


def mark_order_processed(
    order_id,
    lease_token,
    processed_order
):
    """
    Mark the order as successfully processed.
    """

    document_id = get_document_id(order_id)

    document_ref = (
        db.collection(COLLECTION_NAME)
        .document(document_id)
    )

    transaction = db.transaction()

    _complete_order_transaction(
        transaction,
        document_ref,
        lease_token,
        processed_order
    )


@firestore.transactional
def _release_order_transaction(
    transaction,
    document_ref,
    lease_token
):
    """
    Release an order reservation after failure.
    """

    snapshot = document_ref.get(
        transaction=transaction
    )

    if not snapshot.exists:
        return

    data = snapshot.to_dict()

    if data.get("lease_token") == lease_token:
        transaction.delete(document_ref)


def release_order(order_id, lease_token):
    """
    Allow a failed order to be retried.
    """

    document_id = get_document_id(order_id)

    document_ref = (
        db.collection(COLLECTION_NAME)
        .document(document_id)
    )

    transaction = db.transaction()

    _release_order_transaction(
        transaction,
        document_ref,
        lease_token
    )
