import os
import json
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from typing import Any, Optional

from models import WebhookPayload

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
POOL_MAX = int(os.getenv("POOL_MAX", "10"))  # from .env :contentReference[oaicite:2]{index=2}

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------
# Global connection pool
# -------------------------------
pool: Optional[SimpleConnectionPool] = None


def init_pool():
    """Initialize PostgreSQL connection pool"""
    global pool
    if pool is None:
        if not DB_URL:
            raise RuntimeError("DATABASE_URL not set")
        pool = SimpleConnectionPool(minconn=1, maxconn=POOL_MAX, dsn=DB_URL)
        logging.info("✅ PostgreSQL connection pool initialized (maxconn=%s).", POOL_MAX)


def close_pool():
    """Close PostgreSQL connection pool"""
    global pool
    if pool:
        pool.closeall()
        pool = None
        logging.info("✅ PostgreSQL connection pool closed.")


@contextmanager
def get_connection():
    """Get pooled connection with automatic commit/rollback"""
    if pool is None:
        raise RuntimeError("Connection pool is not initialized")
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error("❌ DB transaction failed: %s", e)
        raise
    finally:
        pool.putconn(conn)


# -------------------------------
# Helper casting functions
# -------------------------------
def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except Exception:
        return None


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except Exception:
        return None


# -------------------------------
# Fallback storage for bad payloads
# -------------------------------
def save_failed_payload(raw_payload: dict, error_message: str):
    """Store problematic payloads so no order is ever totally lost."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO failed_payloads (payload, error)
                VALUES (%s, %s)
                """,
                (json.dumps(raw_payload), error_message[:5000]),
            )
            cur.close()
        logging.warning("⚠️ Saved failed payload for later inspection.")
    except Exception as e:
        # If even fallback fails, just log – this is truly exceptional.
        logging.error("❌ Could not save failed payload: %s", e)


# -------------------------------
# Insert logic
# -------------------------------
def insert_data(payload: WebhookPayload):
    """
    Insert a validated WebhookPayload into the normalized database schema.
    Any exception here will cause the worker to log & fallback store the payload.
    """
    if payload.properties is None:
        raise ValueError("Payload.properties is missing")

    props = payload.properties
    restaurant = props.Restaurant
    customer = props.Customer
    order = props.Order

    if restaurant is None or customer is None or order is None:
        raise ValueError("Restaurant/Customer/Order sections are missing in payload")

    tax_list = props.Tax or []
    discount_list = props.Discount or []
    items = props.OrderItem or []
    part_payments = order.part_payments or []

    # Basic derived IDs and casting
    rest_id = restaurant.restID  # text in DB, no cast needed
    order_id = as_int(order.orderID)

    if rest_id is None or order_id is None:
        raise ValueError("Invalid or missing restID/orderID")

    with get_connection() as conn:
        cur = conn.cursor()

        # -------------------------------
        # Restaurant
        # -------------------------------
        cur.execute(
            """
            INSERT INTO restaurants (rest_id, res_name, address, contact_information)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (rest_id) DO NOTHING
            """,
            (
                rest_id,
                restaurant.res_name,
                restaurant.address,
                restaurant.contact_information,
            ),
        )

        # -------------------------------
        # Customer (deduplicated by phone + gstin)
        # -------------------------------
        cur.execute(
            """
            INSERT INTO customers (name, address, phone, gstin)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (phone, gstin) DO UPDATE SET 
                name = EXCLUDED.name,
                address = EXCLUDED.address
            RETURNING customer_id
            """,
            (
                customer.name,
                customer.address,
                customer.phone,
                customer.gstin,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Failed to get customer_id from INSERT/UPDATE")
        customer_id = row[0]

        # -------------------------------
        # Order
        # -------------------------------
        cur.execute(
            """
            INSERT INTO orders (
                order_id, rest_id, customer_id, customer_invoice_id, delivery_charges,
                order_type, payment_type, table_no, no_of_persons, discount_total,
                tax_total, round_off, core_total, total, created_on, order_from,
                order_from_id, sub_order_type, packaging_charge, status, comment,
                biller, assignee
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (order_id, rest_id) DO NOTHING
            """,
            (
                order_id,
                rest_id,
                customer_id,
                order.customer_invoice_id,
                as_float(order.delivery_charges),
                order.order_type,
                order.payment_type,
                order.table_no,
                as_int(order.no_of_persons),
                as_float(order.discount_total),
                as_float(order.tax_total),
                order.round_off,  # stored as text/decimal in DB
                as_float(order.core_total),
                as_float(order.total),
                order.created_on,
                order.order_from,
                order.order_from_id,
                order.sub_order_type,
                as_float(order.packaging_charge),
                order.status,
                order.comment,
                order.biller,
                order.assignee,
            ),
        )

        # -------------------------------
        # Taxes
        # -------------------------------
        for tax in tax_list:
            cur.execute(
                """
                INSERT INTO taxes (order_id, rest_id, title, rate, amount)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, title) DO NOTHING
                """,
                (
                    order_id,
                    rest_id,
                    tax.title,
                    as_float(tax.rate),
                    as_float(tax.amount),
                ),
            )

        # -------------------------------
        # Discounts
        # -------------------------------
        for discount in discount_list:
            cur.execute(
                """
                INSERT INTO discounts (order_id, rest_id, title, type, rate, amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, title) DO NOTHING
                """,
                (
                    order_id,
                    rest_id,
                    discount.title,
                    discount.type,
                    as_float(discount.rate),
                    as_float(discount.amount),
                ),
            )

        # -------------------------------
        # Order Items + Addons
        # -------------------------------
        for item in items:
            item_id = as_int(item.itemid)

            cur.execute(
                """
                INSERT INTO order_items (
                    itemid, order_id, rest_id, name, itemcode, vendoritemcode,
                    specialnotes, price, quantity, total, category_name,
                    sap_code, discount, tax
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (itemid, order_id, rest_id) DO NOTHING
                """,
                (
                    item_id,
                    order_id,
                    rest_id,
                    item.name,
                    item.itemcode,
                    item.vendoritemcode,
                    item.specialnotes,
                    as_float(item.price),
                    as_int(item.quantity),
                    as_float(item.total),
                    item.category_name,
                    item.sap_code,
                    as_float(item.discount),
                    as_float(item.tax),
                ),
            )

            for addon in item.addon or []:
                cur.execute(
                    """
                    INSERT INTO addons (
                        addon_id, itemid, order_id, rest_id, group_name, name,
                        price, quantity, sap_code, addon_group_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (addon_id, itemid, order_id, rest_id) DO NOTHING
                    """,
                    (
                        addon.addon_id,
                        item_id,
                        order_id,
                        rest_id,
                        addon.group_name,
                        addon.name,
                        as_float(addon.price),
                        as_int(addon.quantity),
                        addon.sap_code,
                        addon.addon_group_id,
                    ),
                )

        # -------------------------------
        # Part Payments
        # -------------------------------
        for pp in part_payments:
            cur.execute(
                """
                INSERT INTO part_payments (
                    order_id, rest_id, payment_type, amount, custome_payment_type
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, payment_type, amount) DO NOTHING
                """,
                (
                    order_id,
                    rest_id,
                    pp.payment_type,
                    as_float(pp.amount),
                    pp.custome_payment_type,
                ),
            )

        cur.close()
        logging.info("✅ Insert complete | outlet=%s | order=%s", rest_id, order_id)
