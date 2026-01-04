
# FlexiMart ETL Pipeline 

## Overview

This part of the FlexiMart Data Architecture project focuses on building a robust **ETL pipeline** to clean and load raw CSV data into a relational database (MySQL/PostgreSQL). It also includes **schema documentation** and **business queries** to answer key analytical questions.

The goal is to transform messy, inconsistent raw data into a structured database that supports reliable analytics.

---

## Tasks

1. **Extract**  
   - Read `customers_raw.csv`, `products_raw.csv`, and `sales_raw.csv`.

2. **Transform**  
   - Remove duplicate records.  
   - Handle missing values (drop, fill defaults, or impute).  
   - Standardize phone formats (`+91-XXXXXXXXXX`).  
   - Normalize category names (e.g., "electronics" → "Electronics").  
   - Convert dates to `YYYY-MM-DD`.  
   - Add surrogate keys (auto-increment IDs).

3. **Load**  
   - Insert cleaned data into the `fleximart` database using Python and SQL connectors.  
   - Ensure referential integrity across tables.

---
## Database Schema
Database: **fleximart**

- **customers**: Stores customer details.  
- **products**: Stores product catalog.  
- **orders**: Stores order-level information.  
- **order_items**: Stores line-item details for each order.  

Relationships:  
- One customer → Many orders  
- One order → Many order_items  
- One product → Many order_items  

---
## Business Queries (business_queries.sql)
1. **Customer Purchase History**  
   - Report customers with ≥2 orders and spending > ₹5,000.  
   - Output: `customer_name | email | total_orders | total_spent`

2. **Product Sales Analysis**  
   - Category-level sales summary with product count, quantity sold, and revenue.  
   - Output: `category | num_products | total_quantity_sold | total_revenue`

3. **Monthly Sales Trend (2024)**  
   - Monthly revenue trends with cumulative totals.  
   - Output: `month_name | total_orders | monthly_revenue | cumulative_revenue`

---

## 📑 Data Quality Report (data_quality_report.txt)
Includes:
- Number of records processed per file.  
- Number of duplicates removed.  
- Number of missing values handled.  
- Number of records successfully loaded into the database.  

---

## 🛠️ Technologies Used
- **Python 3.x** (pandas, mysql-connector-python / psycopg2)  
- **MySQL 8.0 / PostgreSQL 14**  
- **SQL** for queries and schema design  

---

## Setup Instructions

1. **Install Python 3.x**  
   Make sure Python 3.x is installed on your system. You can download it from [python.org](https://www.python.org/downloads/).

2. **Clone or Download the Project Files**  
   Place `etl_pipeline.py`, `requirements.txt`, and the three CSV files (`customers_raw.csv`, `products_raw.csv`, `sales_raw.csv`) in your working directory.  
   Ensure the CSV files are located in a folder named `data/` (or update the script paths accordingly).

3. **Configure Database Credentials**  
   Create a `.env` file in your project root with the following variables (update with your actual MySQL credentials):

4. **Python program execution**  
   python part1-database-etl/etl_pipeline.py

---
## Key Learnings

1. Handling real-world data quality issues (duplicates, missing values, inconsistent formats).
2. Designing normalized relational schemas (3NF).
3. Writing analytical SQL queries with joins, aggregates, and window functions.

---