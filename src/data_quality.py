import great_expectations as gx
import pandas as pd


def validate_fact_orders(fact_orders: pd.DataFrame):
    # Load fact_orders
    df = fact_orders.copy()

    # Create GE context
    context = gx.get_context()

    # Create / reuse Pandas datasource
    data_source = context.data_sources.add_pandas("pandas")

    # Create dataframe asset
    data_asset = data_source.add_dataframe_asset(name="fact_orders_asset")

    # Create batch definition and batch
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "fact_orders_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    # Expectations
    expectations = [
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="order_id"
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_order_value",
            min_value=0,
            strict_min=False
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="customer_id"
        )
    ]

    # Run validations
    results = []
    for exp in expectations:
        result = batch.validate(exp)
        results.append(result)

    # Fail pipeline if any expectation fails
    if not all(r.success for r in results):
        raise ValueError("❌ Data Quality Check Failed")

    print("✅ Data Quality Checks Passed")
    return True


if __name__ == "__main__":
    validate_fact_orders()
