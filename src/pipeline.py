import os
from pathlib import Path
from create_tables import create_tables
from transform_data import transform_data
from load_data import load_to_csv, load_to_database

BASE_DIR = Path(__file__).resolve().parents[1]

def run_pipeline(raw_path: str, output_path: str) -> None:
    """
    Orchestrates the complete GreenCart ETL pipeline:
    - Creates database tables
    - Transforms raw CSV data
    - Loads data to CSV and database
    """
    print("Starting GreenCart ETL Pipeline")

    # Step 1: Create required database tables
    print("Creating database tables...")
    create_tables()

    # Step 2: Transform raw data
    print("Transforming raw data...")
    transformed_data = transform_data(raw_path)

    # Step 3: Save transformed data as CSV files
    print("Writing transformed data to CSV")
    load_to_csv(transformed_data, output_path)

    # Step 4: Load data into the database
    print("Loading data into database...")
    load_to_database(transformed_data)

    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    raw_path = Path(
        os.getenv(
            "DATA_PATH",
            str(BASE_DIR / "data" / "raw")
        )
    )

    output_path = BASE_DIR / "data" / "processed"

    run_pipeline(
        raw_path=raw_path,
        output_path=output_path
    )
