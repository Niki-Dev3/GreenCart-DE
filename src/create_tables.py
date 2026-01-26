import os
import pymysql


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", 3306))
    )


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_city VARCHAR(50),
            customer_state VARCHAR(10)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_weight_g INT,
            product_length_cm INT,
            product_height_cm INT,
            product_width_cm INT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_city VARCHAR(50),
            seller_state VARCHAR(10)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_purchase_timestamp DATETIME,
            order_delivered_customer_date DATETIME,
            order_estimated_delivery_date DATETIME,
            total_items INT,
            total_product_value DECIMAL(10,2),
            total_freight_value DECIMAL(10,2),
            total_order_value DECIMAL(10,2),
            payment_value DECIMAL(10,2),
            review_score INT,
            is_late_delivery TINYINT(1),
            bad_review_flag TINYINT(1),
            payment_mismatch TINYINT(1),
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
        );
    """)

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_order_items (
            order_id VARCHAR(50),
            order_item_id INT,
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            price DECIMAL(10,2),
            freight_value DECIMAL(10,2),
            total_item_value DECIMAL(10,2),
            quantity INT
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_tables()
