import os
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from models import WebhookPayload

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

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
pool: SimpleConnectionPool = None

def init_pool():
    """Initialize PostgreSQL connection pool"""
    global pool
    if pool is None:
        pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=DB_URL)
        logging.info("✅ PostgreSQL connection pool initialized.")

def close_pool():
    """Close PostgreSQL connection pool"""
    global pool
    if pool:
        pool.closeall()
        pool = None
        logging.info("✅ PostgreSQL connection pool closed.")

@contextmanager
def get_connection():
    """Get pooled connection"""
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DB Transaction failed: {e}")
        raise
    finally:
        pool.putconn(conn)

# -------------------------------
# Insert logic
# -------------------------------
def insert_data(payload: WebhookPayload):
    with get_connection() as conn:
        cur = conn.cursor()

        restaurant = payload.properties.Restaurant
        customer = payload.properties.Customer
        order = payload.properties.Order
        tax_list = payload.properties.Tax or []
        discount_list = payload.properties.Discount or []
        items = payload.properties.OrderItem or []
        part_payments = order.part_payments or []

        # -------------------------------
        # Restaurant
        # -------------------------------
        cur.execute("""
            INSERT INTO restaurants (rest_id, res_name, address, contact_information)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (rest_id) DO NOTHING
        """, (
            restaurant.restID,
            restaurant.res_name,
            restaurant.address,
            restaurant.contact_information
        ))

        # -------------------------------
        # Customer (deduplicated by phone + gstin)
        # -------------------------------
        cur.execute("""
            INSERT INTO customers (name, address, phone, gstin)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (phone, gstin) DO UPDATE SET 
                name = EXCLUDED.name,
                address = EXCLUDED.address
            RETURNING customer_id
        """, (
            customer.name,
            customer.address,
            customer.phone,
            customer.gstin
        ))
        customer_id = cur.fetchone()[0]

        # -------------------------------
        # Order
        # -------------------------------
        cur.execute("""
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
        """, (
            order.orderID,
            restaurant.restID,
            customer_id,
            order.customer_invoice_id,
            order.delivery_charges,
            order.order_type,
            order.payment_type,
            order.table_no,
            order.no_of_persons,
            order.discount_total,
            order.tax_total,
            order.round_off,
            order.core_total,
            order.total,
            order.created_on,
            order.order_from,
            order.order_from_id,
            order.sub_order_type,
            order.packaging_charge,
            order.status,
            order.comment,
            order.biller,
            order.assignee
        ))

        # -------------------------------
        # Taxes
        # -------------------------------
        for tax in tax_list:
            cur.execute("""
                INSERT INTO taxes (order_id, rest_id, title, rate, amount)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, title) DO NOTHING
            """, (
                order.orderID,
                restaurant.restID,
                tax.title,
                tax.rate,
                tax.amount
            ))

        # -------------------------------
        # Discounts
        # -------------------------------
        for discount in discount_list:
            cur.execute("""
                INSERT INTO discounts (order_id, rest_id, title, type, rate, amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, title) DO NOTHING
            """, (
                order.orderID,
                restaurant.restID,
                discount.title,
                discount.type,
                discount.rate,
                discount.amount
            ))

        # -------------------------------
        # Order Items + Addons
        # -------------------------------
        for item in items:
            cur.execute("""
                INSERT INTO order_items (
                    itemid, order_id, rest_id, name, itemcode, vendoritemcode,
                    specialnotes, price, quantity, total, category_name,
                    sap_code, discount, tax
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (itemid, order_id, rest_id) DO NOTHING
            """, (
                item.itemid,
                order.orderID,
                restaurant.restID,
                item.name,
                item.itemcode,
                item.vendoritemcode,
                item.specialnotes,
                item.price,
                item.quantity,
                item.total,
                item.category_name,
                item.sap_code,
                item.discount,
                item.tax
            ))

            for addon in item.addon or []:
                cur.execute("""
                    INSERT INTO addons (
                        addon_id, itemid, order_id, rest_id, group_name, name,
                        price, quantity, sap_code, addon_group_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (addon_id, itemid, order_id, rest_id) DO NOTHING
                """, (
                    addon.addon_id,
                    item.itemid,
                    order.orderID,
                    restaurant.restID,
                    addon.group_name,
                    addon.name,
                    addon.price,
                    addon.quantity,
                    addon.sap_code,
                    addon.addon_group_id
                ))

        # -------------------------------
        # Part Payments
        # -------------------------------
        for pp in part_payments:
            cur.execute("""
                INSERT INTO part_payments (
                    order_id, rest_id, payment_type, amount, custome_payment_type
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, rest_id, payment_type, amount) DO NOTHING
            """, (
                order.orderID,
                restaurant.restID,
                pp.payment_type,
                pp.amount,
                pp.custome_payment_type
            ))

        cur.close()
        logging.info(f"✅ Insert complete | outlet={restaurant.restID}, order={order.orderID}")
