import os
from typing import Dict

import pandas as pd
import numpy as np
import pymysql


def get_db_connection():
    """
    Create and return a database connection using
    environment variables for configuration.
    """
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", 3306))
    )


def clean_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN and NaT values with None so that
    MySQL / MariaDB can store them as NULL.
    """
    return df.replace({np.nan: None, pd.NaT: None})


def load_to_csv(
    transformed_data: Dict[str, pd.DataFrame],
    output_path: str
):
    """
    Save transformed tables as CSV files.
    Useful for debugging.
    """
    os.makedirs(output_path, exist_ok=True)

    for table_name, df in transformed_data.items():
        file_path = os.path.join(output_path, f"{table_name}.csv")
        df.to_csv(file_path, index=False)
        print(f"📄 CSV saved: {file_path}")


def load_table_to_db(
    df: pd.DataFrame,
    table_name: str,
    connection
):
    """
    Load a DataFrame into MySQL / MariaDB.

    - Removes duplicate rows
    - Converts NaN / NaT to NULL
    - Uses INSERT IGNORE to avoid primary key conflicts
    """
    df = df.drop_duplicates()
    df = clean_for_db(df)

    if df.empty:
        print(f"⚠️ {table_name} is empty, skipping load")
        return

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = (
        f"INSERT IGNORE INTO {table_name}"
        f"({columns}) VALUES ({placeholders})"
    )

    data = [
        tuple(row)
        for row in df.itertuples(index=False, name=None)
    ]

    with connection.cursor() as cursor:
        cursor.executemany(sql, data)

    print(f"⬆️ Loaded {len(data)} rows into {table_name}")


def load_to_database(
    transformed_data: Dict[str, pd.DataFrame]
):
    """
    Load dimension and fact tables into the database
    in the correct order.
    """
    connection = get_db_connection()

    try:
        # Load dimensions first
        for table_name in [
            "dim_customers",
            "dim_products",
            "dim_sellers"
        ]:
            if table_name in transformed_data:
                load_table_to_db(
                    transformed_data[table_name],
                    table_name,
                    connection
                )

        # Load fact tables next
        for table_name in [
            "fact_orders",
            "fact_order_items"
        ]:
            if table_name in transformed_data:
                load_table_to_db(
                    transformed_data[table_name],
                    table_name,
                    connection
                )

        connection.commit()
        print("✅ Database load completed successfully")

    except Exception as e:
        connection.rollback()
        print("❌ Database load failed:", e)
        raise

    finally:
        connection.close()
