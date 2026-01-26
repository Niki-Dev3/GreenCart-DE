import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="GreenCart ETL", layout="wide")

st.title("🛒 GreenCart ETL Pipeline Dashboard")

DATA_PATH = Path("../data/processed")

# Load data
fact_orders = pd.read_csv(DATA_PATH / "fact_orders.csv")

# KPIs
total_revenue = fact_orders["total_order_value"].sum()
total_orders = fact_orders.shape[0]
late_delivery_pct = (
    fact_orders["is_late_delivery"].mean() * 100
)

col1, col2, col3 = st.columns(3)
col1.metric("Total Orders", f"{total_orders:,}")
col2.metric("Total Revenue", f"₹{total_revenue:,.2f}")
col3.metric("Late Delivery %", f"{late_delivery_pct:.2f}%")

st.divider()

# Data preview
st.subheader("📄 Fact Orders Preview")
st.dataframe(fact_orders.head(20))

st.divider()

# Simple visualization
st.subheader("📊 Revenue by State")
revenue_by_state = (
    fact_orders
    .groupby("customer_id")
    .sum(numeric_only=True)["total_order_value"]
)

st.bar_chart(revenue_by_state)
