
# Overview of Star Schema Design Documentation

This documentation guides you through the complete process of designing, implementing, and analyzing the FlexiMart Data Warehouse star schema.

---

## Features & Steps

### 1. Schema Design Documentation 
  ***(`star_schema_design.md`)***

- **Section 1: Schema Overview**
  Presents a clear textual description of the star schema, detailing the fact and dimension tables, their attributes, and relationships. This section establishes the foundation for understanding how transactional sales data is organized for analytical purposes.

- **Section 2: Design Decisions**
  Explains the reasoning behind key modeling choices, including the selection of transaction line-item granularity for maximum analytical flexibility, the use of surrogate keys for data integrity and consistency, and the schema’s support for efficient drill-down and roll-up operations. This section ensures the design is robust, scalable, and aligned with best practices in dimensional modeling.

- **Section 3: Sample Data Flow**
  Demonstrates, through a practical example, how a real-world sales transaction is transformed and loaded into the data warehouse. It illustrates the mapping from source data to the star schema, showing how each component of a transaction populates the fact and dimension tables, and reinforcing the principles of dimensional modeling.

---

### 2. Star Schema Implementation

- **Schema Creation (`warehouse_schema.sql`):**
  - Implements the provided schema for all tables (fact and dimensions) with correct data types, keys, and relationships.
  - Ensures referential integrity with foreign key constraints.

- **Data Population (`warehouse_data.sql`):**
  - Populates the warehouse with realistic sample data:
    - 30 dates (Jan–Feb 2024) in `dim_date`
    - 15 products across 3 categories in `dim_product`
    - 12 customers across 4 cities in `dim_customer`
    - 40 sales transactions in `fact_sales`
  - Follows guidelines for data variety, realism, and correctness.

---

### 3. OLAP Analytics Queries 
***(`analytics_queries.sql`)***

- **Monthly Sales Drill-Down Analysis:**  
  Provides a query to analyze sales performance by year, quarter, and month, demonstrating drill-down capability.

- **Product Performance Analysis:**  
  Identifies top-performing products by revenue, including category, units sold, and revenue contribution percentage.

- **Customer Segmentation Analysis:**  
  Segments customers into High, Medium, and Low value based on total spending, showing counts and revenue per segment.

---

This documentation ensures a clear, step-by-step approach to building and analyzing a robust data warehouse for FlexiMart’s sales analytics needs.

---

