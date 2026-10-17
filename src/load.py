import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def connect_db():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    return connection


if __name__ == "__main__":
    connection = connect_db()

    print("Successfully connected to PostgreSQL!")

    connection.close()

def load_products(products):
    connection = connect_db()

    query = """
        INSERT INTO products (
            product_id, title, category, brand, sku,
            price, discount_percentage, rating, stock,
            availability_status, minimum_order_quantity,
            created_at, updated_at
        )
        VALUES (
            %(product_id)s, %(title)s, %(category)s,
            %(brand)s, %(sku)s, %(price)s,
            %(discount_percentage)s, %(rating)s,
            %(stock)s, %(availability_status)s,
            %(minimum_order_quantity)s,
            %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            brand = EXCLUDED.brand,
            sku = EXCLUDED.sku,
            price = EXCLUDED.price,
            discount_percentage = EXCLUDED.discount_percentage,
            rating = EXCLUDED.rating,
            stock = EXCLUDED.stock,
            availability_status = EXCLUDED.availability_status,
            minimum_order_quantity = EXCLUDED.minimum_order_quantity,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at;
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, products)

        connection.commit()
        

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()