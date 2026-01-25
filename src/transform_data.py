import pandas as pd
from extract_data import extract_data


def auto_convert_dates(df: pd.DataFrame, threshold: float = 0.7) -> pd.DataFrame:
    """
    Automatically detect and convert date or timestamp-like columns
    to datetime where most values are valid.
    """
    date_cols = [
        c for c in df.columns
        if "date" in c.lower() or "timestamp" in c.lower()
    ]

    converted_cols = []

    for col in date_cols:
        converted = pd.to_datetime(df[col], errors="coerce")

        if converted.notna().mean() >= threshold:
            df[col] = converted
            converted_cols.append(col)

    return df


def transform_data(raw_path: str) -> dict:
    """
    Clean, transform, and model raw Olist data into
    analytics-ready dimension and fact tables.
    """
    # -------------------------
    # 1. Extract raw data
    # -------------------------
    data = extract_data(raw_path)

    orders = data["olist_orders_dataset"]
    order_items = data["olist_order_items_dataset"]
    order_payments = data["olist_order_payments_dataset"]
    order_reviews = data["olist_order_reviews_dataset"]
    customers = data["olist_customers_dataset"]
    products = data["olist_products_dataset"]
    sellers = data["olist_sellers_dataset"]

    # -------------------------
    # 2. Basic Data Cleaning
    # -------------------------
    orders = auto_convert_dates(orders, threshold=0.5)
    order_items = auto_convert_dates(order_items, threshold=0.5)
    order_reviews = auto_convert_dates(order_reviews, threshold=0.5)

    # Drop order items without price (cannot contribute to revenue)
    order_items = order_items.dropna(subset=["price"])

    # --------------------------------------------------
    # 2.5 Business rules
    # --------------------------------------------------
    # Revenue is counted only for delivered orders
    orders_delivered = orders[orders["order_status"] == "delivered"].copy()

    # -------------------------
    # 3. Dimension Tables
    # -------------------------
    dim_customers = customers[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    ]].drop_duplicates()

    dim_products = products[[
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]].drop_duplicates()

    dim_sellers = sellers[[
        "seller_id",
        "seller_city",
        "seller_state"
    ]].drop_duplicates()

    # -------------------------
    # 4. Fact Table (Star Schema)
    # -------------------------

    # Aggregate order items to order level
    order_items_agg = (
        order_items
        .groupby("order_id", as_index=False)
        .agg(
            total_items=("order_item_id", "count"),
            total_product_value=("price", "sum"),
            total_freight_value=("freight_value", "sum")
        )
    )

    order_items_agg["total_order_value"] = (
        order_items_agg["total_product_value"]
        + order_items_agg["total_freight_value"]
    )

    # Aggregate payments per order
    payments = (
        order_payments
        .groupby("order_id", as_index=False)
        .agg(payment_value=("payment_value", "sum"))
    )

    # Pick one review per order (highest score if multiple exist)
    reviews_agg = (
        order_reviews
        .sort_values("review_score", ascending=False) 
        .drop_duplicates(subset=["order_id"], keep="first")
    )

    # Build fact_orders (1 row = 1 order)
    fact_orders = (
        orders_delivered
        .merge(order_items_agg, on="order_id", how="left")
        .merge(payments, on="order_id", how="left")
        .merge(
            reviews_agg[["order_id", "review_score"]],
            on="order_id",
            how="left"
        )
    )


    # --------------------------------------------------
    # Derived metrics
    # --------------------------------------------------
    fact_orders["is_late_delivery"] = (
        fact_orders["order_delivered_customer_date"]
        > fact_orders["order_estimated_delivery_date"]
    )

    fact_orders["bad_review_flag"] = fact_orders["review_score"] <= 2

    fact_orders["payment_mismatch"] = (
        fact_orders["payment_value"] < fact_orders["total_order_value"]
    )

    fact_orders = fact_orders[[
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "total_items",
        "total_product_value",
        "total_freight_value",
        "total_order_value",
        "payment_value",
        "review_score",
        "is_late_delivery",
        "bad_review_flag",
        "payment_mismatch"
    ]]

    # --------------------------------------------------
    # Fact table at order-item level
    # --------------------------------------------------
    fact_order_items = (
        orders_delivered
        .merge(order_items, on="order_id", how="inner")
    )

    fact_order_items["total_item_value"] = (
        fact_order_items["price"]
        + fact_order_items["freight_value"]
    )

    fact_order_items["quantity"] = 1

    fact_order_items = fact_order_items[[
        "order_id",
        "order_item_id",
        "customer_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
        "total_item_value",
        "quantity"
    ]]

    # --------------------------------------------------
    # Return transformed datasets
    # --------------------------------------------------
    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_sellers": dim_sellers,
        "fact_orders": fact_orders,
        "fact_order_items": fact_order_items
    }


if __name__ == "__main__":
    transformed = transform_data("../data/raw")
