# 🛒 GreenCart E-Commerce ETL Pipeline

## 📌 Project Overview

GreenCart is a growing e-commerce startup that currently relies on raw CSV exports from its operational systems.  
The sales and analytics teams need **clean, reliable, analytics-ready data** for reporting in tools like **Power BI**.

As the first **Data Engineer**, this project implements a **fully automated ETL pipeline** that:
- Ingests raw CSV data
- Cleans and applies business logic
- Models the data using a **Star Schema**
- Loads data into a relational database
- Enforces **data quality checks**
- Runs automatically via **CI/CD**
- Data Visualization **Streamlit**

---

## 🧱 Architecture Overview
```
Raw CSV Files
↓
Extract (Python)
↓
Transform (Business Logic + Metrics)
↓
Star Schema (Dimensions & Facts)
↓
Load (CSV + MariaDB)
↓
Power BI / Analytics
```

---

## 📂 Project Structure

```
greencart_DE/
│
├── data/
│ ├── raw/
│ ├── processed/
│ ├── sample/
|
|
├── src/
│ ├── create_tables.py
│ ├── extract_data.py
| ├── data_quality.py
│ ├── transform_data.py
│ ├── load_data.py
│ ├── pipeline.py
│ ├── data_visualize.py
│
├── terraform/
│ └── main.tf
│
├── tests/
│ └── test_transform_data.py
│
├── .github/workflows/
│ └── etl.yml
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🗂 Dataset Used

**Olist Brazilian E-Commerce Dataset**  
Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## 🔁 ETL Pipeline Breakdown

### 1️⃣ Extract
- Reads all CSV files dynamically from the raw data folder
- Uses Pandas with safe parsing (`on_bad_lines=skip`)

### 2️⃣ Transform
- Automatic date detection & conversion
- Business rules applied:
  - Only **delivered orders** are considered
  - Aggregates order items and payments
- Metrics calculated:
  - `total_items`
  - `total_product_value`
  - `total_freight_value`
  - `total_order_value`
- Derieved Metrics:
  - `Late delivery`
  - `Bad Review`
  - `Payment mismatch`
- Data modeled into **Star Schema**

### 3️⃣ Load
- Saves transformed tables as CSV (debugging & validation)
- Loads data into **MariaDB**
- Handles NULL values correctly
- Prevents duplicate inserts using `INSERT IGNORE`

---

## ⭐ Star Schema Data Model

### Dimension Tables
- `dim_customers`
- `dim_products`
- `dim_sellers`

### Fact Tables
- `fact_orders`
- `fact_order_items`

### Relationships

<img width="686" height="416" alt="image" src="https://github.com/user-attachments/assets/7c9c06d5-dc4d-41ed-a3d9-98162b07c365" />


This schema is optimized for analytical queries and BI reporting.

---

## ✅ Data Quality Checks (Great Expectations)

Implemented validations:
- No negative prices
- No duplicate order IDs
- Mandatory fields cannot be NULL

The pipeline **fails automatically** if data quality checks fail.

---

## 🧪 Testing

- Unit tests written using **pytest**
- Example:
  - Validates `total_order_value = product + freight`
- Tests run automatically in GitHub Actions

---

## 🔄 CI/CD (GitHub Actions)

On every push or pull request:
- Install dependencies
- Run unit tests
- Execute full ETL pipeline
- Connects to a MariaDB service container

This ensures **code quality and reliability**.

---

## Data Visualization

Using streamlit to visualize the processed data.

<img width="1169" height="512" alt="image" src="https://github.com/user-attachments/assets/dc1f4bd3-a023-4773-827f-73407cb498aa" />


<img width="1172" height="430" alt="image" src="https://github.com/user-attachments/assets/17d5010e-10cf-4ada-b25a-ab99933938cd" />

---

## 🐳 Docker Support

This project is fully containerized using Docker to ensure consistency across different environments.

### Prerequisites
- Docker installed
- Docker Compose installed

---

### 🚀 Run the Project using Docker Compose (Recommended)

Docker Compose is used to orchestrate multiple services (Application + Database).

```
docker compose up --build
```

---

## ☁️ Infrastructure as Code (Terraform)

Terraform is used to define cloud infrastructure declaratively.

### Resources Defined:

- Azure Resource Group
- Azure Storage Account (raw & processed data)
- Azure SQL Database (analytics)

Terraform files are theoretical and demonstrate cloud readiness.

---
## How to Run Locally

### Prerequisites

- Python 3.12.x
- MariaDB

### 1️⃣ Install Dependencies
```
pip install -r requirements.txt
```

### 2️⃣ Set Environment Variables

#### Linux
```
export DB_HOST=localhost
export DB_USER=root
export DB_PASSWORD=rootpass
export DB_NAME=greencart_db
export DATA_PATH=../data/raw
```
#### Windows
```
set DB_HOST=localhost
set DB_USER=root
set DB_PASSWORD=rootpass
set DB_NAME=greencart_db
set DATA_PATH=..\data\raw
```

### 3️⃣ Run Pipeline
```
python src/pipeline.py
```
-or-
```
python3 src/pipeline.py
```
---

## 🧠 Assumptions Made

- Only delivered orders are valid for revenue analysis
- One row in order_items represents one product
- Reviews are deduplicated using the highest score
- Payment mismatches indicate potential issues

---

## 🚀 Azure Deployment Strategy (Future Scope)

- Azure Data Factory for orchestration
- Azure Blob Storage for raw & processed data
- Azure SQL Database for analytics
- Power BI for reporting
- CI/CD via GitHub Actions

---

## 🏁 Conclusion

## This project demonstrates:

- End-to-end ETL pipeline design
- Data modeling best practices
- CI/CD automation
- Data quality enforcement
- Cloud-ready infrastructure design

Built with scalability, reliability, and analytics in mind.
