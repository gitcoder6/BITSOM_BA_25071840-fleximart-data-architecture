use fleximart_dw;

-- Query 1: Monthly Sales Drill-Down
-- Business Scenario: CEO wants yearly, quarterly, monthly sales for 2024
        -- Show: year, quarter, month, total_sales, total_quantity
        -- Group by year, quarter, month
        -- Order chronologically
        -- The below statement demonstrates drill-down (Year → Quarter → Month)
            -- GROUP BY d.year, d.quarter, d.month, d.month_name

SELECT
    d.year,
    d.quarter,
    d.month_name,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity_sold) AS total_quantity
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.quarter, d.month;



-- Query 2: Top 10 Products by Revenue
-- Business Scenario: Identify top-performing products
        -- Join fact_sales with dim_product
        -- Calculate: total revenue, total quantity per product
        -- Calculate: percentage of total revenue ((each product's revenue / overall revenue) × 100)
        -- Order by revenue descending
        -- Limit to top 10

WITH product_revenue AS (
    SELECT
        p.product_name,
        p.category,
        SUM(f.quantity_sold) AS units_sold,
        ROUND(SUM(f.total_amount), 0) AS revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY p.product_name, p.category
),
total AS (
    SELECT SUM(revenue) AS total_rev FROM product_revenue
)
SELECT
    pr.product_name,
    pr.category,
    pr.units_sold,
    pr.revenue,
    CONCAT(ROUND((pr.revenue / t.total_rev) * 100, 1), '%') AS revenue_percentage
FROM product_revenue pr, total t
ORDER BY pr.revenue DESC
LIMIT 10;



-- Query 3: Customer Segmentation Analysis
-- Business Scenario: Segment customers into High/Medium/Low value
        -- Calculate total spending per customer
        -- Use CASE statement to create segments ( 'High Value' (>₹50,000 spent), 'Medium Value' (₹20,000-₹50,000), and 'Low Value' (<₹20,000))
        -- Group by segment (High/Medium/Low value customers)
        -- Order by Segment (High/Medium/Low value)


WITH customer_spending AS (
    SELECT
        c.customer_name,
        SUM(f.total_amount) AS total_spent
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_name
)
SELECT
    CASE
        WHEN total_spent > 50000 THEN 'High Value'
        WHEN total_spent BETWEEN 20000 AND 50000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent), 0) AS total_revenue,
    ROUND(AVG(total_spent), 0) AS avg_revenue_per_customer
FROM customer_spending
GROUP BY customer_segment
ORDER BY
    CASE customer_segment
        WHEN 'High Value' THEN 1
        WHEN 'Medium Value' THEN 2
        ELSE 3
    END;