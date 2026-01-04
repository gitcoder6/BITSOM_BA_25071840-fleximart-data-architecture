# # FlexiMart ETL Pipeline
# 
# This python code demonstrates a robust ETL(Extraction-Transform-Load) pipeline for FlexiMart's customer, product, and sales data.
# 
# Particulars
# - Data cleaning, validation, and transformation
# - Loading cleaned data into MySQL database tables (with validation and error handling)
# - Logging at every step and saving logs to .log file
# 

# 
# 1. Imports, Logger, and Configuration
# 
# Imports all required libraries, sets up logging, and loads database credentials from the `.env` file.
# 

import subprocess
import sys
import os


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to requirements.txt
requirements_path = os.path.join(script_dir, "requirements.txt")

# Path to data folder
data_folder = os.path.join(script_dir, "..", "data")

# Path for log file and data quality report
log_file_path = os.path.join(script_dir, "etl_pipeline.log")
data_quality_report_path = os.path.join(script_dir, "data_quality_report.txt")


def install_requirements():
    
    # Install requirements
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_path])


install_requirements()

import os

import pandas as pd
import numpy as np
import phonenumbers
import mysql.connector
import logging

from dotenv import load_dotenv
from datetime import datetime

# Suppress SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

# -------------------- SETUP LOGGER --------------------
# Set up logging to record ETL process events and errors for debugging and auditing
logging.basicConfig(
    filename=log_file_path,  # Log file name
    level=logging.INFO,           # Log level: INFO records all major events
    format='%(asctime)s %(levelname)s:%(message)s'  # Log format with timestamp
)


# Create a logger object
logger = logging.getLogger()

# Load environment variables from .env file in the root directory
load_dotenv()

#
# ## 2. Utility Functions
#
# This module defines helper functions used throughout the ETL pipeline.
# They handle common data cleaning, validation, and database operations.
#
# Functions included:
# 1. strip_id_prefix        – Remove the first character from Customer, Product, and Transaction IDs
#                             (e.g., 'C001' → '001', 'P003' → '003').
# 2. trim_string_columns    – Trim leading/trailing spaces from all string columns in a DataFrame.
# 3. format_phone_number    – Standardize phone numbers to +91-XXXXXXXXXX format.
# 4. normalize_category_name– Standardize product category names (e.g., 'electronic' → 'Electronics').
# 5. normalize_date_format  – Convert various date formats into YYYY-MM-DD.
# 6. strip_spaces           – Remove leading/trailing spaces from a single string value.
# 7. impute_product_data_median – Fill missing product attributes (price, stock_quantity) with median values.
# 8. impute_missing_emails  – Replace missing emails with a placeholder: unknown_email_{customer_id}.
# 9. validate_dataframe_schema – Validate DataFrame structure and required columns.
# 10. reset_csv_file        – Clear contents of a CSV file if it exists.
# 11. connect_to_database   – Establish a MySQL database connection using .env credentials.
# 12. build_quality_report  – Generate a data quality report (records processed, duplicates, missing values, etc.).
#

# Function to remove the first character from a string value
def strip_id_prefix(val):
    """
    Remove the first character from a string identifier.
    Used to convert surrogate keys like 'C001' → '001'.
    
    Parameters:
        val (str): Input string value.
    
    Returns:
        str or np.nan: Processed string without first character, or NaN if invalid.
    """
    try:
        if pd.isna(val):
            return np.nan
        return str(val)[1:]
    except Exception as e:
        logger.error(f"Error removing first character from '{val}': {e}")
        return np.nan

# Function to trim leading/trailing spaces from all string columns in a DataFrame
def trim_string_columns(df):
    """
	Strip leading and trailing whitespace from all string columns in a DataFrame.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with cleaned string columns.
    """
    try:
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            df[col] = df[col].str.strip()
        logger.info("Trimmed string columns in DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Error trimming string columns: {e}")
        return df

# Function to standardize phone numbers
def format_phone_number(phone):
    """
    Standardize phone numbers to the format: +<country_code>-XXXXXXXXXX.
    
    Rules:
        - Removes spaces, dashes, and leading zeros.
        - Ensures country code (defaults to India: "IN").
        - Validates using the `phonenumbers` library.
    
    Parameters:
        phone (str): Raw phone number string.
    
    Returns:
        str: Standardized phone number in +91-XXXXXXXXXX format.
        np.nan: If invalid or cannot be parsed.
    """
    try:
        parsed = phonenumbers.parse(phone, "IN")
        if phonenumbers.is_valid_number(parsed):
            cc = parsed.country_code
            national = str(parsed.national_number)[-10:]  # Last 10 digits
            phonenumber = f'+{cc}-{national}'
            return phonenumber
        else:
            logger.warning(f"Invalid phone number: {phone}")
            return np.nan
    except Exception as e:
        logger.error(f"Error formatting phone number '{phone}': {e}")
        return np.nan

# Function to standardize product category names
def normalize_category_name(cat):
    """
    Normalize product category names.
    
    Rules:
        - Converts to lowercase, strips spaces.
        - Maps keywords to standard categories:
            'electronic' → 'Electronics'
            'fashion'   → 'Fashion'
            'grocer'    → 'Groceries'
        - Otherwise returns Title Case of the input.
    
    Parameters:
        cat (str): Raw category name.
    
    Returns:
        str: Standardized category name.
        None: If input is NaN or invalid.
    """
    try:
        if pd.isna(cat):
            return None
        cat = cat.strip().lower()
        if 'electronic' in cat:
            return 'Electronics'
        elif 'fashion' in cat:
            return 'Fashion'
        elif 'grocer' in cat:
            return 'Groceries'
        else:
            return cat.title()
    except Exception as e:
        logger.error(f"Error standardizing category '{cat}': {e}")
        return None

# Function to standardize date formats to YYYY-MM-DD
def normalize_date_format(date_str):
    """
    Convert various date formats into a standard YYYY-MM-DD format.
    
    Approach:
        - Attempts parsing with common formats first.
        - Falls back to pandas `to_datetime` if needed.
    
    Parameters:
        date_str (str): Input date string.
    
    Returns:
        str: Date in YYYY-MM-DD format.
        np.nan: If parsing fails.
    """
    try:
        # Try parsing with known formats first
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except:
                continue
        # Fallback to pandas parsing
        return pd.to_datetime(date_str, errors='coerce', dayfirst=False).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {e}")
        return np.nan

# Function to clean leading/trailing spaces from a string value
def strip_spaces(val):
    """
    Remove leading and trailing spaces from a string value.
    
    Parameters:
        val (str or any): Input value.
    
    Returns:
        str: Trimmed string if input is a string.
        Original value: If not a string.
    """
    try:
        if isinstance(val, str):
            return val.strip()
        else:
            return val
    except Exception as e:
        logger.error(f"Error cleaning spaces for '{val}': {e}")
        return val

# Function to fill missing product data (Price and stock_quantity) with median values
def impute_product_data_median(df):
    """
    Fill missing product attributes with median values.
    
    Columns affected:
        - 'price'
        - 'stock_quantity'
    
    Parameters:
        df (pd.DataFrame): Products DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with missing values replaced by column medians.
    """
    try:
        df['price'] = df['price'].fillna(df['price'].median())
        df['stock_quantity'] = df['stock_quantity'].fillna(df['stock_quantity'].median())
        logger.info("Filled missing product data with median values.")
        return df
    except Exception as e:
        logger.error(f"Error filling missing product data: {e}")
        return df

# Function to fill missing email addresses with a placeholder
# replaced missing email ids with "unknown_email_<custid>" to make it unique
def impute_missing_email(customers_df):
    """
    Fill missing email addresses with unique placeholders.
    
    Rule:
        Replace missing or empty emails with "unknown_email_<customer_id>".
    
    Parameters:
        customers_df (pd.DataFrame): Customers DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with filled email addresses.
    """
    try:
        customers_df['email'] = customers_df.apply(
            lambda row: f"unknown_email_{row['customer_id']}" if pd.isnull(row['email']) or row['email'] == '' else row['email'],
            axis=1
        )
        logger.info("Filled missing emails with placeholders.")
        return customers_df
    except Exception as e:
        logger.error(f"Error filling missing emails: {e}")
        return customers_df

# Function to validate DataFrame structure and content
def validate_df(df, expected_columns, df_name):
    """
    Validate DataFrame structure and content.
    
    Checks:
        - DataFrame is not None.
        - DataFrame is not empty.
        - All expected columns are present.
    
    Parameters:
        df (pd.DataFrame): DataFrame to validate.
        expected_columns (list): List of required column names.
        df_name (str): Name of the DataFrame (for logging).
    
    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        if df is None:
            logger.error(f"{df_name} is None.")
            return False
        if df.empty:
            logger.error(f"{df_name} is empty.")
            return False
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"{df_name} is missing columns: {missing_cols}")
            return False
        logger.info(f"{df_name} validated successfully.")
        return True
    except Exception as e:
        logger.error(f"Error validating DataFrame {df_name}: {e}")
        return False

# Function to clean CSV file if it exists
def reset_csv_file(filepath):
    """
    Clear contents of a CSV file if it exists.
    
    Parameters:
        filepath (str): Path to the CSV file.
    
    Returns:
        None
    """
    try:
        if os.path.exists(filepath):
            open(filepath, 'w').close()
            logger.info(f"Cleaned existing file: {filepath}")
    except Exception as e:
        logger.error(f"Error cleaning file {filepath}: {e}")

# Function to get database connection
def connect_to_db():
    """
    Establish a MySQL database connection using credentials from a .env file.
    
    Steps:
        - Load environment variables (DB_HOST, DB_USER, DB_PASS, DB_NAME).
        - Validate credentials.
        - Attempt connection using mysql.connector.
    
    Returns:
        mysql.connector.connection.MySQLConnection: Active DB connection.
        None: If connection fails.
    """
    try:
        # Load environment variables from .env file in the root directory
        load_dotenv()
        # Read DB credentials from environment variables
        host = os.getenv("DB_HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        database = os.getenv("DB_NAME")
        logger.info(f"Database credentials read: host={host}, user={user}, database={database}")

        # Validate credentials
        if not all([host, user, password, database]):
            logger.error("Missing database credentials in .env file.")
            return None

        # Connect to MySQL
        conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        logger.info("Database connection established successfully.")
        return conn

    except mysql.connector.Error as err:
        logger.error(f"Database connection error: {err}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during DB connection: {e}")
        return None

# Function to create all required tables
def initialize_db_tables(conn):
    """
    Create required MySQL tables if they do not already exist.
    
    Tables:
        - customers
        - products
        - orders
        - order_items
    
    Parameters:
        conn (mysql.connector.connection.MySQLConnection): Active DB connection.
    
    Returns:
        None
    """
    # Define table creation queries
    table_queries = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY AUTO_INCREMENT,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            city VARCHAR(50),
            registration_date DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY AUTO_INCREMENT,
            product_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock_quantity INT DEFAULT 0
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY AUTO_INCREMENT,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT PRIMARY KEY AUTO_INCREMENT,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """
    ]
    # Execute table creation queries
    try:
        # Create a cursor object
        cursor = conn.cursor()
        # Execute each table creation query
        for query in table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("All tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        cursor.close()

# Function to generate data quality report
def build_quality_report(df, file_name):
    """
    Generate a data quality report for a given DataFrame.
    
    Metrics:
        - Records processed
        - Duplicates removed
        - Missing values handled
        - Records loaded successfully
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        file_name (str): Name of the source file.
    
    Returns:
        dict: Summary report with quality metrics.
    """
    # Total Number of records processed
    records_processed = len(df)
    logger.info(f"Records processed in {file_name}: {records_processed}")
    
    # Number of duplicates removed
    duplicates_removed = df.duplicated().sum()
    logger.info(f"Duplicates removed in {file_name}: {duplicates_removed}")
    
    # Number of missing values handled
    missing_values_handled = df.isnull().sum().sum()
    logger.info(f"Missing values handled in {file_name}: {missing_values_handled}")
    
    # Number of records dropped due to duplicates and missing values
    if file_name == 'customers_raw.csv' or file_name == 'products_raw.csv':
        cleaned_df = df.drop_duplicates()
    else:
        cleaned_df = df.drop_duplicates().dropna()
    logger.info(f"Number of records after cleaning in {file_name}: {len(cleaned_df)}")

    # Number of records loaded successfully
    records_loaded_successfully = len(cleaned_df)
    logger.info(f"Records loaded successfully from {file_name}: {records_loaded_successfully}")

    # Compile report
    return {
        "File": file_name,
        "Records Processed": records_processed,
        "Duplicates Removed": duplicates_removed,
        "Missing Values Handled": missing_values_handled,
        "Records Loaded Successfully": records_loaded_successfully
    }

# Function to get CSV file path
def get_csv_path(filename):
    """Return full path of a CSV file from the data folder."""
    return os.path.join(data_folder, filename)

# Loading the CSV file in DataFrame with error handling
def load_csv(file_path):
    """Load a CSV file into a pandas DataFrame with error handling."""
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()
        df = pd.read_csv(file_path)
        logger.info(f"Loaded file successfully: {os.path.basename(file_path)} (Rows: {len(df)}, Columns: {len(df.columns)})")
        return df
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        return pd.DataFrame()
        
# 
# ## 3. Extract Raw Data
# 
# Read the raw CSV files from the `data` folder.
# 

# -------------------- EXTRACT LOGIC --------------------
# -------------------- READ CUSTOMER RAW DATA --------------------


# -------------------- Extract Logic Function --------------------
logger.info("------------------------ Date Extract Logic from CSV File -----------------")
# Function to extract raw data from CSV files
def extract_raw_data():
    """
     Read raw CSV files for customers, products, and sales.
    
    Files:
        - customers_raw.csv
        - products_raw.csv
        - sales_raw.csv
    
    Returns:
        tuple: (customers, products, sales, customers_raw, products_raw, sales_raw)
               Each as a DataFrame (empty if load fails).
   """
    customers_csv_path = get_csv_path("customers_raw.csv")
    products_csv_path = get_csv_path("products_raw.csv")
    sales_csv_path = get_csv_path("sales_raw.csv")

    customers_raw = load_csv(customers_csv_path)
    customers = customers_raw.copy()
    products_raw = load_csv(products_csv_path)
    products = products_raw.copy()
    sales_raw = load_csv(sales_csv_path)
    sales = sales_raw.copy()
   
    return customers, products, sales, customers_raw, products_raw, sales_raw

# ## 4. Transform or Clean Data
# 
# Clean all Customer raw, product raw, sales raw CSV file and split the Sales Raw clean dataset to Order and Order Items CSV file and load the data into MySQL database
# 


# -------------------- TRANSFORM LOGIC --------------------
# -------------------- CLEAN CUSTOMERS --------------------

# Clean customers data: remove duplicates, handle missing emails, standardize phone/city/date
def clean_customers(customers_df):
    """
    Clean and standardize customers dataset.
    
    Steps:
        1. Normalize surrogate keys (customer_id).
        2. Trim whitespace in string columns.
        3. Remove duplicate customers.
        4. Fill missing emails with placeholders.
        5. Standardize phone numbers.
        6. Normalize city names to Title Case.
        7. Convert registration_date to YYYY-MM-DD.
        8. Reorder columns to match SQL schema.
        9. Load cleaned data into MySQL.
    
    Parameters:
        customers_df (pd.DataFrame): Raw customers DataFrame.
    
    Returns:
        pd.DataFrame: Cleaned customers DataFrame.

    """
    try:             
        # Remove first character from customer_id for surrogate key
        customers_df['customer_id'] = customers_df['customer_id'].apply(strip_id_prefix)
        logger.info("Surrogate customer_id created in customers.csv.")
        
        # Trim string columns (assuming trim_string_columns trims all string columns in df)
        customers_df = trim_string_columns(customers_df)
        logger.info("Trimmed string columns in customers DataFrame.")
        
        # Remove duplicate customers based on customer_id
        customers_df = customers_df.drop_duplicates(subset=['customer_id'])
        logger.info(f"Customer duplicates removed. Remaining records: {len(customers_df)}")
        
        # Remove rows where 'email' is NaN or empty
        customers_df = impute_missing_email(customers_df)
        logger.info("Missing emails filled with placeholder values.")

        # Standardize phone numbers
        customers_df['phone'] = customers_df['phone'].apply(format_phone_number)
        logger.info("Phone numbers standardized.")

        # Standardize city names to Title Case
        customers_df['city'] = customers_df['city'].str.title()
        logger.info("City names standardized in Title Case.")

        # Standardize registration dates to YYYY-MM-DD format
        customers_df['registration_date'] = customers_df['registration_date'].apply(normalize_date_format)
        logger.info("Registration dates standardized in YYYY-MM-DD format.")

        # Trim string columns again after transformations
        customers_df = trim_string_columns(customers_df)
        logger.info("String columns trimmed of leading/trailing spaces.")

        logger.info(f"Cleaned customers data. Shape: {customers_df.shape}")

        # Reorder columns to match SQL table
        customers = customers_df[['customer_id', 'first_name', 'last_name', 'email', 'phone', 'city', 'registration_date']]
        logger.info("Customer data cleaning complete.")        

        # Load cleaned customers data into the database
        logger.info("--------- Loading data to Customer table initiated ---------")
        load_data_to_customers_db(customers_df)

        return customers
    except Exception as e:
        logger.error(f"Error cleaning customers data: {e}")

# Clean customers data: remove duplicates, handle missing emails, standardize phone/city/date
#customers_clean = clean_customers(customers)

# -------------------- CLEAN PRODUCTS --------------------
# Clean products data: remove duplicates, handle missing prices/stock, standardize category/name
def clean_products(products_df):
    """
    Clean and standardize products dataset.
    
    Steps:
        1. Normalize surrogate keys (product_id).
        2. Trim whitespace in string columns.
        3. Remove duplicate products.
        4. Fill missing price and stock_quantity with median values.
        5. Standardize category names.
        6. Clean product names (trim spaces).
        7. Reorder columns to match SQL schema.
        8. Load cleaned data into MySQL.
    
    Parameters:
        products_df (pd.DataFrame): Raw products DataFrame.
    
    Returns:
        pd.DataFrame: Cleaned products DataFrame.

    """
    try:  
        # Remove first character from product_id for surrogate key
        products_df['product_id'] = products_df['product_id'].apply(strip_id_prefix)
        logger.info("Surrogate product_id created in products.csv.")

        # Trim string columns (assuming trim_string_columns trims all string columns in df)
        products_df = trim_string_columns(products_df)
        
        # Remove duplicate products based on product_id
        products_df = products_df.drop_duplicates(subset=['product_id'])
        logger.info(f"Product duplicates removed. Remaining records: {len(products_df)}")

        # Fill missing values with median        
        logger.info("Missing stock quantities filled with median.")
        products_df = impute_product_data_median(products_df)        
        logger.info("Missing prices filled from sales_raw.csv mapping.")

        # Standardize category names to Title Case
        products_df['category'] = products_df['category'].apply(normalize_category_name)
        logger.info("Category names standardized in Title Case.")

        # Standardize product names (trim spaces)
        products_df['product_name'] = products_df['product_name'].apply(strip_spaces)
        logger.info("Product names trimmed of leading/trailing spaces.")

        # Trim string columns again after transformations
        logger.info("String columns trimmed of leading/trailing spaces.")
        products_df = trim_string_columns(products_df)        

        logger.info(f"Cleaned products data. Shape: {products_df.shape}")

        # Reorder columns to match SQL table
        products = products_df[['product_id', 'product_name', 'category', 'price', 'stock_quantity']]
        logger.info("Product data cleaning complete.")
        
        # Load cleaned products data into the database
        logger.info("--------- Loading data to Product table initiated ---------")
        load_data_to_products_db(products_df)

        return products
    except Exception as e:
        logger.error(f"Error cleaning products data: {e}")

# Clean products data: remove duplicates, handle missing prices/stock, standardize category/name
#products_clean = clean_products(products)

# -------------------- CLEAN SALES --------------------
# Clean sales data: remove duplicates, handle missing IDs, standardize date
def clean_sales(sales_df):    
    """
    Clean and standardize sales dataset.
    
    Steps:
        1. Normalize surrogate keys (customer_id, product_id, transaction_id).
        2. Trim whitespace in string columns.
        3. Remove duplicate transactions.
        4. Drop rows missing customer_id or product_id.
        5. Convert transaction_date to YYYY-MM-DD.
    
    Parameters:
        sales_df (pd.DataFrame): Raw sales DataFrame.
    
    Returns:
        pd.DataFrame: Cleaned sales DataFrame.

    """
    try:
        
        # Remove first character from customer_id, product_id, transaction_id for surrogate keys
        sales_df['customer_id'] = sales_df['customer_id'].apply(strip_id_prefix)
        sales_df['product_id'] = sales_df['product_id'].apply(strip_id_prefix)

        sales_df['transaction_id'] = sales_df['transaction_id'].apply(strip_id_prefix)
        logger.info("Surrogate transaction_id created in sales.csv.")

        # Trim string columns (assuming trim_string_columns trims all string columns in df)
        sales_df = trim_string_columns(sales_df)
        logger.info("Trimmed string columns in sales DataFrame.")

        # Remove duplicate products based on product_id
        sales_df = sales_df.drop_duplicates(subset=['transaction_id'])
        logger.info(f"Sales duplicates removed. Remaining records: {len(sales_df)}")

        # Drop rows with missing customer_id or product_id
        sales_df = sales_df.dropna(subset=['customer_id'])
        sales_df = sales_df.dropna(subset=['product_id'])
        logger.info(f"Dropped rows with missing customer_id or product_id. Remaining records: {len(sales_df)}")   
             
        # Standardize transaction_date to YYYY-MM-DD format
        sales_df['transaction_date'] = sales_df['transaction_date'].apply(normalize_date_format)
        logger.info(f"Cleaned sales data. Shape: {sales_df.shape}")
        
        logger.info("Sales data cleaning complete.")
        return sales_df
    except Exception as e:
        logger.error(f"Error cleaning sales data: {e}")
        return pd.DataFrame()

# Clean sales data: remove duplicates, handle missing IDs, standardize date
#sales_clean = clean_sales(sales)  

# -------------------- SPLIT ORDERS --------------------
# Split sales data into orders and order_items DataFrames
def split_orders(sales_clean):
    """
    Split cleaned sales data into orders dataset.
    
    Steps:
        1. Copy sales_clean DataFrame.
        2. Generate surrogate order_id from transaction_id.
        3. Calculate total_amount (quantity × unit_price).
        4. Create orders DataFrame with required columns.
        5. Save to orders.csv.
        6. Load into MySQL orders table.
    
    Parameters:
        sales_clean (pd.DataFrame): Cleaned sales DataFrame.
    
    Returns:
        pd.DataFrame: Orders DataFrame.

    """
    try:    
        # Copy sales_clean to avoid modifying original DataFrame
        logger.info("Starting split of sales into orders.")
        df = sales_clean.copy()
        logger.info("Copied sales_clean DataFrame.")

        # Create a unique order_id for each transaction
        df['order_id'] = df['transaction_id']
        logger.info("Generated surrogate order_id for each transaction.")

        # Calculate total_amount for each order (quantity * unit_price)
        df['total_amount'] = df['quantity'] * df['unit_price']
        logger.info("Calculated total_amount for each order.")

        # Orders DataFrame
        orders = df[['order_id', 'customer_id', 'transaction_date', 'total_amount', 'status']].drop_duplicates()
        logger.info(f"Created orders DataFrame with shape: {orders.shape}")

        # Rename columns to match SQL table structure
        orders = orders.rename(columns={
            'order_id': 'order_id',
            'customer_id': 'customer_id',
            'transaction_date': 'order_date',
            'total_amount': 'total_amount',
            'status': 'status'
        })
        logger.info("Renamed columns in orders DataFrame to match SQL structure.")

        # Clean existing CSV before saving Orders DataFrame:
        csv_path = data_folder + '/orders.csv'
        reset_csv_file(csv_path)

        # Save to CSV
        orders.to_csv(csv_path, index=False)
        logger.info("Saved orders DataFrame to {csv_path}")

        logger.info(f"Split sales into orders ({orders.shape})")

        # Load cleaned orders data into the database
        logger.info("--------- Loading data to Order table initiated ---------")
        load_data_to_orders_db(orders)
        return orders
    except Exception as e:
        logger.error(f"Error splitting sales data: {e}")
        return pd.DataFrame()

# Split sales data into orders and order_items DataFrames
#order_clean = split_orders(sales_clean)


# -------------------- SPLIT ORDER ITEMS --------------------
def split_sales_to_order_items(sales_clean):
    
    """
    Split cleaned sales data into order_items dataset.
    
    Steps:
        1. Copy sales_clean DataFrame
    """
    try:
        
        # Copy sales_clean to avoid modifying original DataFrame
        logger.info("Starting split of sales into order_items.")
        order_items = sales_clean.copy()

        # Create a unique order_item_id for each transaction (same as in orders)
        order_items['order_item_id'] = order_items.index + 1
        logger.info("Generated surrogate order_item_id for each transaction.")
        
        # Calculate total_amount for each order (quantity * unit_price)
        order_items['subtotal'] = order_items['quantity'] * order_items['unit_price']
        logger.info("Calculated subtotal for each order item.")

        # Order Items DataFrame
        order_items = order_items[['order_item_id', 'transaction_id', 'product_id', 'quantity', 'unit_price', 'subtotal']].copy()
        logger.info(f"Created order_items DataFrame with shape: {order_items.shape}")

        # Rename columns to match SQL table structure
        order_items = order_items.rename(columns={
            'order_item_id': 'order_item_id',
            'transaction_id': 'order_id',
            'product_id': 'product_id',
            'quantity': 'quantity',
            'unit_price': 'unit_price',
            'subtotal': 'subtotal'
        })
        logger.info("Renamed columns in order_items DataFrame to match SQL structure.")

        # Before saving your DataFrame:
        csv_path = data_folder + '/order_items.csv'
        reset_csv_file(csv_path)

        # Save to CSV
        order_items.to_csv(csv_path, index=False)
        logger.info("Saved order_items DataFrame to {csv_path}")

        logger.info(f"Split sales into order_items ({order_items.shape})")
        
        # Load cleaned order_items data into the database
        logger.info("--------- Loading data to Order Item table initiated ---------")
        load_data_to_order_items_db(order_items)
        return order_items
    except Exception as e:
        logger.error(f"Error splitting sales data: {e}")
        return pd.DataFrame()

# Split sales data into orders and order_items DataFrames
#order_items_df = split_sales_to_order_items(sales_clean)

# ## 5. Functions for loading the data into different MYSQL tables (Customers, Products, Orders and Order_items)
# 1. It validate the Dataframe
# 2. If table not exists then create the table
# 3. Insert the data into tables


# -------------------- LOAD LOGIC --------------------
# -------------------- LOADING CUSTOMERS DATA INTO DATABASE --------------------

# Function to load customers data into the database
def load_data_to_customers_db(customers_df: pd.DataFrame):    
    """
    Load cleaned customers data into the MySQL database.

    Workflow:
        1. Validate that the DataFrame is not empty.
        2. Establish a database connection using environment credentials.
        3. Ensure required tables exist (create if missing).
        4. Delete existing customers and dependent records (orders, order_items) 
           to avoid foreign key conflicts.
        5. Insert new customer records into the 'customers' table.

    Parameters:
        customers_df (pd.DataFrame): Cleaned customers dataset.

    Returns:
        None
    """
    logger.info("Starting to load customers data into database.")

    # Validate DataFrame
    if customers_df is None or customers_df.empty:
        logger.error("Customers DataFrame is empty or None. Aborting load.")
        return    

    # SQL Query to insert customer data
    query_insert = """
        INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # Establish DB Connection
    conn = connect_to_db()
    if conn is None:
        logger.error("Failed to establish database connection.")
        return

    # Create all tables before loading data
    initialize_db_tables(conn)  # Ensure tables exist before loading data
    
    # Perform DB operations
    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Delete existing rows from customers and dependent tables to avoid FK issues
        logger.info("Deleting existing customers and dependent orders data...")
        cursor.execute("DELETE FROM order_items")  # To avoid FK constraint issues
        cursor.execute("DELETE FROM orders")  # To avoid FK constraint issues
        cursor.execute("DELETE FROM customers")

        # Prepare data for insertion
        customers = customers_df.values.tolist()
        logger.info(f"Preparing to insert {len(customers)} customer records.")
        
        # Insert customer records
        cursor.executemany(query_insert, customers)
        conn.commit()
        logger.info("Customers data loaded successfully.")

    except mysql.connector.Error as err:
        logger.error(f"Error loading customers: {err}")
    except Exception as e:
        logger.error(f"Unexpected error loading customers: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")


# -------------------- LOADING PRODUCTS DATA INTO DATABASE --------------------

# Function to load products data into the database
def load_data_to_products_db(products_df: pd.DataFrame):    
    """
    Load cleaned products data into the MySQL database.

    Workflow:
        1. Validate that the DataFrame is not empty.
        2. Establish a database connection using environment credentials.
        3. Ensure required tables exist (create if missing).
        4. Delete existing product records to avoid duplication.
        5. Insert new product records into the 'products' table.

    Parameters:
        products_df (pd.DataFrame): Cleaned products dataset.

    Returns:
        None

    """

    logger.info("Starting to load products data into database.")

    # Validate DataFrame
    if products_df is None or products_df.empty:
        logger.error("Products DataFrame is empty or None. Aborting load.")
        return    

    # SQL Query to insert product data
    query_insert = """
        INSERT INTO products (product_id, product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Establish DB Connection
    conn = connect_to_db()
    if conn is None:
        logger.error("Failed to establish database connection.")
        return

    # Perform DB operations
    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Delete existing rows from products and dependent order_items tables to avoid FK issues
        logger.info("Deleting existing products data...")
        cursor.execute("DELETE FROM order_items")  # To avoid FK constraint issues
        cursor.execute("DELETE FROM products")

        # Prepare data for insertion
        
        products = products_df.values.tolist()
        logger.info(f"Preparing to insert {len(products)} product records.")
        
        # Insert product records
        cursor.executemany(query_insert, products)
        conn.commit()
        logger.info("Products data loaded successfully.")

    except mysql.connector.Error as err:
        logger.error(f"Error loading products: {err}")
    except Exception as e:
        logger.error(f"Unexpected error loading products: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")


# -------------------- LOADING ORDERS DATA INTO DATABASE --------------------

# Function to load orders data into the database
def load_data_to_orders_db(orders_df: pd.DataFrame):
    
    """
    Load cleaned products data into the MySQL database.

    Workflow:
        1. Validate that the DataFrame is not empty.
        2. Establish a database connection using environment credentials.
        3. Ensure required tables exist (create if missing).
        4. Delete existing product records to avoid duplication.
        5. Insert new product records into the 'products' table.

    Parameters:
        products_df (pd.DataFrame): Cleaned products dataset.

    Returns:
        None
    """

    logger.info("Starting to load orders data into database.")
    # Validate DataFrame
    if orders_df is None or orders_df.empty:
        logger.error("Orders DataFrame is empty or None. Aborting load.")
        return


    # SQL Query to insert order data
    query_insert = """
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Establish DB Connection
    conn = connect_to_db()
    if conn is None:
        logger.error("Failed to establish database connection.")
        return

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Delete existing rows from orders table
        logger.info("Deleting existing orders data...")
        cursor.execute("DELETE FROM orders")

        # Prepare data for insertion        
        orders = orders_df.values.tolist()
        logger.info(f"Preparing to insert {len(orders)} order records.")

        # Insert order records
        cursor.executemany(query_insert, orders)
        conn.commit()
        logger.info("Orders data loaded successfully.")

    except mysql.connector.Error as err:
        logger.error(f"Error loading orders: {err}")
    except Exception as e:
        logger.error(f"Unexpected error loading orders: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")


# -------------------- LOADING ORDER_ITEMS DATA INTO DATABASE --------------------

# Function to load order items data into the database
def load_data_to_order_items_db(order_items_df: pd.DataFrame):
    """
    Load cleaned order_items data into the MySQL database.

    Workflow:
        1. Validate that the DataFrame is not empty.
        2. Establish a database connection using environment credentials.
        3. Ensure required tables exist (create if missing).
        4. Delete existing order_items records to avoid duplication.
        5. Insert new order_items records into the 'order_items' table.

    Parameters:
        order_items_df (pd.DataFrame): Cleaned order_items dataset.

    Returns:
        None
    """
    logger.info("Starting to load order items data into database.")
    
    # Validate DataFrame
    if order_items_df is None or order_items_df.empty:
        logger.error("Order Items DataFrame is empty or None. Aborting load.")
        return

    # SQL Query to insert order item data
    query_insert = """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    # Establish DB Connection
    conn = connect_to_db()
    if conn is None:
        logger.error("Failed to establish database connection.")
        return

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Delete existing rows from order_items table
        logger.info("Deleting existing order items data...")
        cursor.execute("DELETE FROM order_items")

        # Prepare data for insertion
        
        orders_items = order_items_df.values.tolist()
        logger.info(f"Preparing to insert {len(orders_items)} order item records.")

        # Insert order item records
        cursor.executemany(query_insert, orders_items)
        conn.commit()
        logger.info("Order items data loaded successfully.")
    except mysql.connector.Error as err:
        logger.error(f"Error loading order items: {err}")
    except Exception as e:
        logger.error(f"Unexpected error loading order items: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")
   
# 
# ## 6. Data Quality Report
# 
# Print a summary of the ETL process for data quality assurance.
# - Number of records processed per file
# - Number of duplicates removed
# - Number of missing values handled
# - Number of records loaded successfully
# 


# Generate report for each file

def write_data_quality_report(customers, products, sales_raw):
    """
    Persist a data quality report to disk.

    Workflow:
        1. Accepts a report dictionary generated by `build_quality_report`.
        2. Writes the report to the specified file path (CSV, JSON, or text).
        3. Ensures file is created or overwritten safely.

    Parameters:
        report (dict): Data quality metrics (records processed, duplicates removed, etc.).
        filepath (str): Destination file path for saving the report.

    Returns:
        None
    """
    report = []
    report.append(build_quality_report(customers, "customers_raw.csv"))
    report.append(build_quality_report(products, "products_raw.csv"))
    report.append(build_quality_report(sales_raw, "sales_raw.csv"))

    with open(data_quality_report_path, "w") as f:
        f.write("Data Quality Report (ETL Summary):\n\n")
        for r in report:
            f.write(f"File: {r['File']}\n")
            f.write(f"- Records Processed: {r['Records Processed']}\n")
            f.write(f"- Duplicates Removed: {r['Duplicates Removed']}\n")
            f.write(f"- Missing Values Handled: {r['Missing Values Handled']}\n")
            f.write(f"- Records Loaded Successfully: {r['Records Loaded Successfully']}\n\n")

# -------------------- MAIN ETL PIPELINE --------------------
def main():
    

    # 1. Extract Raw Data
    logger.info("---------------- Data Extraction started from CSV files ----------------")
    customers, products, sales, customers_raw, products_raw, sales_raw = extract_raw_data()
    logger.info("---------------- Data Extraction Ended from CSV files ------------------")

    # 2. Transform and Load Data
    logger.info("---------------- Data Transformation started ---------------------------")
    clean_customers(customers)
    clean_products(products)
    sales_clean = clean_sales(sales)
    logger.info("---------------- Data Transformation Ended -----------------------------")

    # 3. Split sales into orders and order_items
    logger.info("---------------- Data Splitting started for Order and Order Item ----------------")
    split_orders(sales_clean)
    split_sales_to_order_items(sales_clean)
    logger.info("---------------- Data Splitting Ended for Order and Order Item ----------------")
    
    # 4. Generate Data Quality Report
    logger.info("---------------- Quality Report Generation started ----------------------")
    write_data_quality_report(customers_raw, products_raw, sales_raw)
    logger.info("Data quality report generated at data_quality_report.txt.")
    logger.info("---------------- Quality Report Generation Ended ----------------------")

# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    main()

logger.info("------- ETL Pipeline Complete ---------------")
# ETL Pipeline Complete!