USE fleximart_dw;

-- Insert date data into dim_date table between January-February 2024
INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, month, month_name, quarter, year, is_weekend)
WITH RECURSIVE date_cte AS (
    SELECT DATE('2024-01-01') AS full_date
    UNION ALL
    SELECT DATE_ADD(full_date, INTERVAL 1 DAY)
    FROM date_cte
    WHERE full_date < '2024-02-29'
)
SELECT 
    DATE_FORMAT(full_date, '%Y%m%d') AS date_key,
    full_date,
    DAYNAME(full_date) AS day_of_week,
    DAY(full_date) AS day_of_month,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    CONCAT('Q', QUARTER(full_date)) AS quarter,
    YEAR(full_date) AS year,
    CASE WHEN DAYOFWEEK(full_date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_cte;

-- Insert 40 products (3 categories) into dim_product table
INSERT INTO dim_product (product_id, product_name, category, subcategory, unit_price) VALUES
('P001', 'Samsung S25 Ultra', 'Electronics', 'Mobile Phones', 95000),
('P002', 'Macbook Pro 15', 'Electronics', 'Laptops', 85000),
('P003', 'Boat Earbuds', 'Electronics', 'Audio', 2500),
('P004', 'Samsung Galaxy Watch', 'Electronics', 'Wearables', 12000),
('P005', 'X box Console', 'Electronics', 'Gaming', 40000),
('P006', 'Samsung 4K LED TV', 'Electronics', 'Television', 65000),
('P007', 'Logitech Wireless Mouse', 'Electronics', 'Accessories', 800),
('P008', 'Logitech Mechanical Keyboard', 'Electronics', 'Accessories', 3500),
('P009', 'Boat Portable Speaker', 'Electronics', 'Audio', 2200),
('P010', 'Seagate External Hard Drive 1TB', 'Electronics', 'Storage', 4500),
('P011', 'Sony Digital Camera', 'Electronics', 'Cameras', 30000),
('P012', 'Drone Pro', 'Electronics', 'Drones', 75000),
('P013', 'VR Headset', 'Electronics', 'Gaming', 18000),
('P014', 'Power Bank 20000mAh', 'Electronics', 'Accessories', 1500),
('P015', 'Smart Home Hub', 'Electronics', 'Smart Devices', 9000),
('P016', 'Mens Leather Jacket', 'Fashion', 'Outerwear', 5500),
('P017', 'Womens Designer Dress', 'Fashion', 'Dresses', 7500),
('P018', 'Adidas Shoes', 'Fashion', 'Footwear', 3200),
('P019', 'Duke T-Shirt', 'Fashion', 'Tops', 1500),
('P020', 'Raymond Suit', 'Fashion', 'Suits', 12000),
('P021', 'Silk Saree', 'Fashion', 'Ethnic Wear', 8500),
('P022', 'Denim Jeans', 'Fashion', 'Bottoms', 1800),
('P023', 'Leather Handbag', 'Fashion', 'Accessories', 4500),
('P024', 'Woolen Sweater', 'Fashion', 'Outerwear', 2200),
('P025', 'Designer Sunglasses', 'Fashion', 'Accessories', 3500),
('P026', 'Luxury Watch', 'Fashion', 'Accessories', 25000),
('P027', 'Cotton Kurta', 'Fashion', 'Ethnic Wear', 1200),
('P028', 'Leather Belt', 'Fashion', 'Accessories', 800),
('P029', 'High Heels', 'Fashion', 'Footwear', 2800),
('P030', 'Winter Jacket', 'Fashion', 'Outerwear', 6000),
('P031', 'Basmati Rice 5kg', 'Groceries', 'Grains', 600),
('P032', 'Organic Wheat Flour 10kg', 'Groceries', 'Grains', 450),
('P033', 'Olive Oil 1L', 'Groceries', 'Oils', 800),
('P034', 'Almonds 500g', 'Groceries', 'Dry Fruits', 650),
('P035', 'Green Tea Pack', 'Groceries', 'Beverages', 300),
('P036', 'Fresh Apples 1kg', 'Groceries', 'Fruits', 150),
('P037', 'Milk 2L', 'Groceries', 'Dairy', 100),
('P038', 'Cheddar Cheese 500g', 'Groceries', 'Dairy', 400),
('P039', 'Breakfast Cereals', 'Groceries', 'Packaged Food', 250),
('P040', 'Honey 500g', 'Groceries', 'Condiments', 350);

-- Insert 24 customers (6 cities) into dim_customer table
INSERT INTO dim_customer (customer_id, customer_name, city, state, customer_segment) VALUES
('C001', 'Amit Sharma', 'Jaipur', 'Rajasthan', 'Consumer'),
('C002', 'Priya Verma', 'Delhi', 'Delhi', 'Corporate'),
('C003', 'Rajesh Kumar', 'Mumbai', 'Maharashtra', 'Home Office'),
('C004', 'Neha Singh', 'Calcutta', 'West Bengal', 'Consumer'),
('C005', 'Vikram Patel', 'Chennai', 'Tamil Nadu', 'Corporate'),
('C006', 'Sneha Iyer', 'Bangalore', 'Karnataka', 'Consumer'),
('C007', 'Arjun Mehta', 'Jaipur', 'Rajasthan', 'Small Business'),
('C008', 'Kavita Joshi', 'Delhi', 'Delhi', 'Home Office'),
('C009', 'Rohan Das', 'Mumbai', 'Maharashtra', 'Consumer'),
('C010', 'Meera Nair', 'Calcutta', 'West Bengal', 'Corporate'),
('C011', 'Siddharth Rao', 'Chennai', 'Tamil Nadu', 'Small Business'),
('C012', 'Anjali Gupta', 'Bangalore', 'Karnataka', 'Consumer'),
('C013', 'Manish Kapoor', 'Jaipur', 'Rajasthan', 'Corporate'),
('C014', 'Pooja Reddy', 'Delhi', 'Delhi', 'Home Office'),
('C015', 'Deepak Choudhary', 'Mumbai', 'Maharashtra', 'Consumer'),
('C016', 'Ritika Malhotra', 'Calcutta', 'West Bengal', 'Small Business'),
('C017', 'Karan Bansal', 'Chennai', 'Tamil Nadu', 'Corporate'),
('C018', 'Shruti Desai', 'Bangalore', 'Karnataka', 'Consumer'),
('C019', 'Harish Menon', 'Jaipur', 'Rajasthan', 'Home Office'),
('C020', 'Sonal Jain', 'Delhi', 'Delhi', 'Small Business'),
('C021', 'Aditya Ghosh', 'Mumbai', 'Maharashtra', 'Consumer'),
('C022', 'Tanvi Kulkarni', 'Calcutta', 'West Bengal', 'Corporate'),
('C023', 'Ramesh Yadav', 'Chennai', 'Tamil Nadu', 'Home Office'),
('C024', 'Divya Arora', 'Bangalore', 'Karnataka', 'Small Business');

-- Insert 80 randomly generated sales transactions into fact_sales table
INSERT INTO fact_sales (date_key, product_key, customer_key, quantity_sold, unit_price, discount_amount, total_amount)
SELECT
    date_key,
    product_key,
    customer_key,
    qty AS quantity_sold,
    unit_price,
    disc AS discount_amount,
    qty * unit_price - disc AS total_amount
FROM (
    SELECT
        d.date_key,
        p.product_key,
        c.customer_key,
        CASE 
            WHEN d.is_weekend = TRUE THEN FLOOR(RAND() * 3 + 3)
            ELSE FLOOR(RAND() * 3 + 1)
        END AS qty,
        p.unit_price,
        ROUND(p.unit_price * (RAND() * 0.10), 2) AS disc
    FROM dim_date d
    CROSS JOIN dim_product p
    CROSS JOIN dim_customer c
    ORDER BY RAND()
    LIMIT 80
) AS t;