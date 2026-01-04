# FlexiMart Data Architecture Project

**Student Name:** Maneesh Singhal  
**Student ID:** BITSoM_BA_25071840  
**Email:** singhal.maneesh@gmail.com  
**Date:** 04-01-2026

---

## Project Overview
This project implements a complete **AI-driven data architecture** for FlexiMart, an e-commerce company. It covers building the full pipeline from raw CSV ingestion to advanced analytics, including:

- **Part 1:** ETL pipeline, relational database schema, and business queries.  
- **Part 2:** NoSQL analysis and MongoDB implementation.  
- **Part 3:** Data warehouse design, star schema implementation, and OLAP queries.  

The project demonstrates how to handle messy real-world data, design normalized schemas, leverage NoSQL for flexibility, and build a dimensional warehouse for analytics.

---
## Repository Structure
```
├── data/
│   ├── customers_raw.csv
│   ├── products_raw.csv
│   ├── sales_raw.csv
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── schema_documentation.md
│   ├── business_queries.sql
│   ├── data_quality_report.txt
│   ├── README.md
│   └── requirements.txt
├── part2-nosql/
│   ├── nosql_analysis.md
│   ├── mongodb_operations.py
│   ├── README.md
│   └── products_catalog.json
├── part3-datawarehouse/
│   ├── star_schema_design.md
│   ├── warehouse_schema.sql
│   ├── warehouse_data.sql
│   ├── README.md
│   └── analytics_queries.sql
├── .gitignore
└── README.md
```
---
## Technologies Used
- **Python 3.14** (pandas, mysql-connector-python / psycopg2)  
- **MySQL 8.0 / PostgreSQL 14**  
- **MongoDB 6.0**  
- **SQL** for relational and warehouse queries  

---
## Setup Instruction
### Database setup
```bash
# Create databases
mysql -u root -p -e "CREATE DATABASE fleximart;"
mysql -u root -p -e "CREATE DATABASE fleximart_dw;"

# Run Part 1 - ETL Pipeline
python part1-database-etl/etl_pipeline.py

# Run Part 1 - Business Queries
mysql -u root -p fleximart < part1-database-etl/business_queries.sql

# Run Part 3 - Data Warehouse
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql

### MongoDB Operations
python part2-nosql/mongodb_operations.py
```
---
## Key Learnings
- How to design and implement ETL pipelines for messy real-world data.
- Importance of database normalization (3NF) for consistency and integrity.
- Benefits of NoSQL (MongoDB) for flexible product catalogs and embedded reviews.
- Building a star schema for OLAP and enabling drill-down/roll-up analytics.

---
## Challenges Faced
#### 1. Data Consistency during ETL
- **Challenge:** Ensuring consistency between raw CSV files and MySQL database.  
- **Solution:** Implemented robust data validation and logging mechanisms to maintain data quality.

#### 2. MongoDB Connection Management**
- **Challenge:** Handling connection errors and resource cleanup.  
- **Solution:** Used error handling and context managers to manage database connections safely.

#### 3. Optimizing Star Schema Design**
- **Challenge:** Designing a schema that supports complex analytical queries efficiently.  
- **Solution:** Applied dimensional modeling principles and validated performance through query testing.
