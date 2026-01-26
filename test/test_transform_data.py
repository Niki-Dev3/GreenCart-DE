import pandas as pd
from transform_data import transform_data


def test_total_order_value_calculation(tmp_path):
    """
    Ensure total_order_value = product + freight
    """

    # Arrange: fake minimal dataset
    orders = pd.DataFrame({
        "order_id": ["00143d0f86d6fbd9f9b38ab440ac16f5"],
        "customer_id": ["c1"],
        "order_status": ["delivered"],
        "order_purchase_timestamp": ["2023-01-01"],
        "order_estimated_delivery_date": ["2023-01-05"],
        "order_delivered_customer_date": ["2023-01-04"]
    })

    order_items = pd.DataFrame([
        {
        "order_id": "00143d0f86d6fbd9f9b38ab440ac16f5",
        "order_item_id": 1,
        "product_id": "p1",
        "seller_id": "s1",
        "price": 21.33,
        "freight_value": 15.10
    },
    {
        "order_id": "00143d0f86d6fbd9f9b38ab440ac16f5",
        "order_item_id": 2,
        "product_id": "p1",
        "seller_id": "s1",
        "price": 21.33,
        "freight_value": 15.10
    },
    {
        "order_id": "00143d0f86d6fbd9f9b38ab440ac16f5",
        "order_item_id": 3,
        "product_id": "p1",
        "seller_id": "s1",
        "price": 21.33,
        "freight_value": 15.10
    }
    ])

    payments = pd.DataFrame({
        "order_id": ["00143d0f86d6fbd9f9b38ab440ac16f5"],
        "payment_value": [109.29]
    })

    reviews = pd.DataFrame({
        "order_id": ["00143d0f86d6fbd9f9b38ab440ac16f5"],
        "review_score": [5]
    })

    customers = pd.DataFrame({
        "customer_id": ["c1"],
        "customer_unique_id": ["cu1"],
        "customer_city": ["SP"],
        "customer_state": ["SP"]
    })

    products = pd.DataFrame({
        "product_id": ["p1"],
        "product_category_name": ["electronics"],
        "product_weight_g": [500],
        "product_length_cm": [10],
        "product_height_cm": [5],
        "product_width_cm": [8]
    })

    sellers = pd.DataFrame({
        "seller_id": ["s1"],
        "seller_city": ["SP"],
        "seller_state": ["SP"]
    })

    # Monkeypatch extract_data
    def mock_extract_data(_):
        return {
            "olist_orders_dataset": orders,
            "olist_order_items_dataset": order_items,
            "olist_order_payments_dataset": payments,
            "olist_order_reviews_dataset": reviews,
            "olist_customers_dataset": customers,
            "olist_products_dataset": products,
            "olist_sellers_dataset": sellers,
        }

    import transform_data as td
    td.extract_data = mock_extract_data

    # Act
    result = transform_data("dummy_path")
    fact_orders = result["fact_orders"]

    # Assert
    assert round(fact_orders.iloc[0]["total_order_value"],2) == 109.29
