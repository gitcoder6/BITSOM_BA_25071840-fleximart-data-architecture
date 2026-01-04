
"""
MongoDB Operations for Product Catalog
--------------------------------------
This script demonstrates MongoDB operations:
1. Load data from JSON
2. Query products under a price threshold
3. Analyze reviews for high ratings
4. Update product reviews
5. Aggregate average price by category
"""


import subprocess
import sys

# Function to install requirements from requirements.txt
def install_requirements():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "../part1-database-etl/requirements.txt"])

# Install requirements
install_requirements()

import os
import json
import logging
from datetime import datetime, timezone
from pymongo import MongoClient, errors
from dotenv import load_dotenv
from typing import List, Dict, Tuple

# -------------------- CONFIGURATION --------------------
LOG_FILE = "mongodb_operations.log"

# -------------------- LOGGER SETUP --------------------
# Set up logging to record mongodb operations events and errors for debugging and auditing
logging.basicConfig(
    filename=LOG_FILE,  
    level=logging.INFO,           
    format='%(asctime)s %(levelname)s:%(message)s'  
)

# Create a logger object
logger = logging.getLogger()

# -------------------- END SETUP LOGGER --------------------

# --------------- MONGODB CONNECTION SETUP --------------------
# Function to connect to MongoDB
def connect_to_mongodb() -> Tuple[MongoClient, any]:
    """
    Connect to MongoDB using credentials from .env file.
    Returns the MongoDB client and the specified database and collection.
    """
    # Load MongoDB URI from .env file
    
    # Load environment variables from .env file
    load_dotenv()   

    # Mongo DB URI and Database/Collection names from environment variables
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DB = os.getenv("MONGODB_DB")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    
    # Initialize MongoDB client
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI not found in Environment Variable file.")
    
    # Create MongoDB client
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    
    # Access the specified database and collection
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    logger.info(f"MongoDB client initialized: uri={MONGODB_URI}, db={MONGODB_DB}, collection={collection.name}")
    try:     
        # Test connection
        client.admin.command('ping')
        logger.info(f"Connected to MongoDB successfully. {client.server_info()}")
        return client, collection
    except errors.ServerSelectionTimeoutError as err:
        logger.error(f"Could not connect to MongoDB: {err}")
        raise
    except Exception as e:
        logger.error(f"Error during MongoDB setup: {e}")
        raise
# -------------------- END MONGODB CONNECTION SETUP --------------------

# -------------------- Load Data in MongoDB OPERATIONS --------------------
# 
# ## Operation 1: Load Data
# 
# Load product data from `products_catalog.json` into the MongoDB `products` collection.  
# Existing data is cleared for repeatability.
# 

# function to load data from JSON file into MongoDB
def load_data(json_path: str, collection) -> None:
    """
    Load products from JSON file into MongoDB collection.
    Clears the collection before inserting new data.
    """
    try:
        # Read products from JSON file
        with open(json_path, 'r') as f:
            products = json.load(f)
        # Remove existing documents for a clean start
        delete_result = collection.delete_many({})
        logger.info(f"Deleted {delete_result.deleted_count} existing documents from collection.")
        
        # Insert new documents
        insert_result = collection.insert_many(products)
        logger.info(f"Inserted {len(insert_result.inserted_ids)} products into collection.")
    #  Handle potential errors
    except FileNotFoundError:
        logger.error(f"File {json_path} not found.")
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error during data load: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# -------------------- END Load Data in MongoDB OPERATIONS --------------------

# -------------------- MongoDB OPERATION for Basic Queries --------------------
# 
# ## Operation 2: Basic Query
# 
# Find all products in the "Electronics" category with price less than 50,000.  
# Only display the product name, price, and stock.
# 


# Function to query electronics products under 50000
def query_electronics_under_50000(collection) -> List[Dict]:
    """
    Find electronics products priced under 50000.
    Returns a list of dicts with name, price, and stock.
    """
    try:
        # Define the query and projection
        query = {"category": "Electronics", "price": {"$lt": 50000}} 
        projection = {"_id": 0, "name": 1, "price": 1, "stock": 1} 
        
        # Execute the query
        results = list(collection.find(query, projection))
        logger.info("---------------------- Query Results Data ---------------------------")
        logger.info(f"Found {len(results)} electronics products under 50000.")

        # Return results
        return results
    #  Handle potential errors
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error during query: {e}")
        return []
    except Exception as e:
        logger.error(f"Query error: {e}")
        return []


# -------------------- END MongoDB OPERATION for Basic Queries --------------------

# -------------------- MongoDB OPERATION for Review Analysis --------------------
# 
# ## Operation 3: Review Analysis
# 
# Find all products that have an average rating of 4.0 or higher.  
# Uses aggregation to calculate the average rating from the reviews array.
# 


# Function to find products with average review rating >= 4.0 using aggregation
def products_with_high_avg_rating(collection, min_rating: float = 4.0) -> List[Dict]:
    """
    Find products with average review rating >= 4.0.
    Use aggregation to calculate average from reviews array.
    """
    try:
        # Define the aggregation pipeline
        pipeline = [
            {"$unwind": "$reviews"}, # Break down the reviews array into individual documents
            {"$group": {
                "_id": "$product_id", # Group by product ID
                "name": {"$first": "$name"}, # Get the product name
                "avg_rating": {"$avg": "$reviews.rating"} # Calculate average rating
            }},
            {"$match": {"avg_rating": {"$gte": min_rating}}}, # Filter for average rating >= min_rating
            {"$project": {"_id": 0, "product_id": "$_id", "name": 1, "avg_rating": 1}} # Project desired fields
        ]
        # Execute the aggregation pipeline
        results = list(collection.aggregate(pipeline))
        logger.info("---------------------- Query Results Data ---------------------------")
        logger.info(f"Found {len(results)} products with average rating >= {min_rating}.")

        # Return results
        return results
    #  Handle potential errors
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error during aggregation: {e}")
        return []
    except Exception as e:
        logger.error(f"Aggregation error: {e}")
        return []


# -------------------- END MongoDB OPERATION for Review Analysis --------------------


# -------------------- MongoDB OPERATION for Update Opearion --------------------
# 
# ## Operation 4: Update Operation
# 
# Add a new review to the product with `product_id` "ELEC001".
# 

# Function to add a review to a product
def update_review(collection, product_id, user_id, username, rating, comment):
    """
    Add a review to a product.
    Returns 1 if successful, 0 otherwise.
    """
    # Create the review document
    review = {
        "user_id": user_id, # User ID of the reviewer
        "username": username, # Username of the reviewer
        "rating": rating, # Rating given by the reviewer
        "comment": comment, # Review comment
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d") # Current date in UTC
    }
    try:
        # Update the product document by pushing the new review into the reviews array
        result = collection.update_one(
            {"product_id": product_id},
            {"$push": {"reviews": review}}
        )
        # Log the result
        if result.modified_count:
            logger.info("---------------------- Query Results Data ---------------------------")
            logger.info(f"New review added to product {product_id}, review: {review}")
        else:
            logger.warning(f"Product {product_id} not found or review not added.")
        
        # Return number of modified documents
        return result.modified_count, result
    #  Handle potential errors
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error during update: {e}")
        return 0
    except Exception as e:
        logger.error(f"Update error: {e}")
        return 0

# -------------------- END MongoDB OPERATION for Update Opearion --------------------

# -------------------- MongoDB OPERATION for Complex Aggregation --------------------
# 
# ## Operation 5: Complex Aggregation
# 
# Calculate the average price and product count for each category.  
# Results are sorted by average price in descending order.
# 


# Function to calculate average price by category
def avg_price_by_category(collection) -> List[Dict]:
    """
    Calculate average price and product count by category.
    Returns a list of dicts with category, avg_price, and product_count.
    """
    try:
        # Define the aggregation pipeline
        pipeline = [
            {"$group": {
                "_id": "$category", # Group by category
                "avg_price": {"$avg": "$price"}, # Calculate average price
                "product_count": {"$sum": 1} # Count products in each category
            }},
            {"$project": {
                "_id": 0, # Exclude _id from output
                "category": "$_id", # Category name
                "avg_price": 1, # Average price
                "product_count": 1 # Product count
            }},
            {"$sort": {"avg_price": -1}} # Sort by average price descending
        ]
        # Execute the aggregation pipeline
        results = list(collection.aggregate(pipeline))
        logger.info("---------------------- Query Results Data ---------------------------")
        logger.info(f"Calculated average price by category. Results: {len(results)} categories.")
        return results
    #  Handle potential errors
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error during aggregation: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []

# -------------------- END MongoDB OPERATION for Complex Aggregation --------------------

# -------------------- MAIN Method --------------------
def main():   
    try:
        # Connect to MongoDB
        client, collection = connect_to_mongodb()
        # Operation 1: Import the provided JSON file into collection 'products' in MongoDB
        logger.info("\n--- Loading data into MongoDB ---")
        # Load data and log result
        load_data("products_catalog.json", collection)
        logger.info(f"Data loaded into MongoDB successfully. {collection.count_documents({})} documents in collection.")

        # Operation 2: Basic Query 
        # Find all products in "Electronics" category with price less than 50000
        # Return only: name, price, stock
        logger.info("\n--- Electronics under 50000 ---")
        # Query and log results
        electronics = query_electronics_under_50000(collection)
        # Log results
        for product in electronics:
            logger.info(product)

        # Operation 3: Review Analysis
        # Find all products with average rating >= 4.0
        # Use aggregation to calculate average from reviews array
        logger.info("\n--- Products with average rating >= 4.0 ---")
        # Query and log results
        high_rated = products_with_high_avg_rating(collection)
        # Log results
        for product in high_rated:
            logger.info(product)

        # Operation 4: Update Operation
        # Add a new review to product "ELEC001"
        # Review: {user: "U999", rating: 4, comment: "Good value", date: ISODate()}        
        logger.info("\n--- Update review to product ELEC001 ---")
        # Add review and log result
        updated_reviews = update_review(collection, "ELEC001", "U999", "ValueSeeker", 4, "Good value")
        # Log result
        logger.info("Review added." if updated_reviews else "Product not found or review not added.")

        # Operation 5: Complex Aggregation
        # Calculate average price by category
        # Return: category, avg_price, product_count
        # Sort by avg_price descending
        logger.info("\n--- Average price by category ---")
        # Calculate and print average price by category
        category_stats = avg_price_by_category(collection)
        # Print category statistics
        for stat in category_stats:
            logger.info(stat)
    except Exception as e:
        print(f"Fatal error: {e}")
        logger.error(f"Fatal error: {e}")
    
    finally:
            if client:
                client.close()
                logger.info("MongoDB connection closed.")


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    main()