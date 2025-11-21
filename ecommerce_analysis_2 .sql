-- uploaded the csv files under one schema
-- read one kaggle guy on kaggle who created a master table by connecting customer ids, 
-- using union all, I am going to combine customer data to the orders, products, and shipping

-- Step 1: Consolidate the two tables for each entity using UNION ALL
WITH Consolidated_Customers AS (
    SELECT * FROM personal_analysis.df_Customers
    UNION ALL
    -- Updated to use the renamed table: df_Customers1
    SELECT * FROM personal_analysis.df_Customers1
),
Consolidated_Orders AS (
    SELECT * FROM personal_analysis.df_Orders
    UNION ALL
    -- Updated to use the renamed table: df_Orders1
    SELECT * FROM personal_analysis.df_Orders1
),
Consolidated_OrderItems AS (
    SELECT * FROM personal_analysis.df_OrderItems
    UNION ALL
    -- Updated to use the renamed table: df_OrderItems1
    SELECT * FROM personal_analysis.df_OrderItems1
),
Consolidated_Products AS (
    SELECT * FROM personal_analysis.df_Products
    UNION ALL
    -- Updated to use the renamed table: df_Products1
    SELECT * FROM personal_analysis.df_Products1
),
Consolidated_Payments AS (
    SELECT * FROM personal_analysis.df_Payments
    UNION ALL
    -- Updated to use the renamed table: df_Payments1
    SELECT * FROM personal_analysis.df_Payments1
)

-- Step 2: Perform the relational INNER JOINs to create the final master table
SELECT
    -- 1. All columns from ORDERS (O)
    O.*,

    -- 2. Columns from CUSTOMERS (C) - Excluding customer_id
    C.customer_zip_code_prefix,
    C.customer_city,
    C.customer_state,

    -- 3. Columns from ORDER ITEMS (OI) - Excluding order_id and product_id
    OI.seller_id,
    OI.price,
    OI.shipping_charges,

    -- 4. Columns from PRODUCTS (P) - Excluding product_id
    P.product_category_name,
    P.product_weight_g,
    P.product_length_cm,
    P.product_height_cm,
    P.product_width_cm,

    -- 5. Columns from PAYMENTS (PM) - Excluding order_id
    PM.payment_sequential,
    PM.payment_type,
    PM.payment_installments,
    PM.payment_value

FROM
    Consolidated_Orders AS O
INNER JOIN
    Consolidated_Customers AS C
    ON O.customer_id = C.customer_id
INNER JOIN
    Consolidated_OrderItems AS OI
    ON O.order_id = OI.order_id
INNER JOIN
    Consolidated_Products AS P
    ON OI.product_id = P.product_id
INNER JOIN
    Consolidated_Payments AS PM
    ON O.order_id = PM.order_id;
    ## got an error 1222, Use AI to fix it

-- Phase 1: Consolidated Tables (CTEs) - Corrected to use available columns
-- Phase 1: Consolidated Tables (CTEs) - ONLY Consolidated_Orders is changed
WITH Consolidated_Customers AS (
    SELECT
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM personal_analysis.df_Customers
    UNION ALL
    SELECT
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM personal_analysis.df_Customers1
),

Consolidated_Orders AS (
    -- SELECT for df_Orders: Adds NULL values for missing status/delivery columns
    SELECT
        order_id,
        customer_id,
        NULL AS order_status, -- NULL for missing column
        order_purchase_timestamp,
        order_approved_at,
        NULL AS order_delivered_timestamp, -- NULL for missing column
        NULL AS order_estimated_delivery_date -- NULL for missing column
    FROM personal_analysis.df_Orders
    
    UNION ALL
    
    -- SELECT for df_Orders1: Uses the actual column names
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_timestamp,
        order_estimated_delivery_date
    FROM personal_analysis.df_Orders1
),

Consolidated_OrderItems AS (
    SELECT
        order_id,
        product_id,
        seller_id,
        price,
        shipping_charges
    FROM personal_analysis.df_OrderItems
    UNION ALL
    SELECT
        order_id,
        product_id,
        seller_id,
        price,
        shipping_charges
    FROM personal_analysis.df_OrderItems1
),

Consolidated_Products AS (
    SELECT
        product_id,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM personal_analysis.df_Products
    UNION ALL
    SELECT
        product_id,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM personal_analysis.df_Products1
),

Consolidated_Payments AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM personal_analysis.df_Payments
    UNION ALL
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM personal_analysis.df_Payments1
)

-- Phase 2: Final Master Table Creation (No changes needed here)
SELECT
    O.order_id,
    O.customer_id,
    O.order_status,
    O.order_purchase_timestamp,
    O.order_approved_at,
    O.order_delivered_timestamp,
    O.order_estimated_delivery_date,
    C.customer_zip_code_prefix,
    C.customer_city,
    C.customer_state,
    OI.seller_id,
    OI.price AS item_price,
    OI.shipping_charges,
    P.product_category_name,
    P.product_weight_g,
    P.product_length_cm,
    P.product_height_cm,
    P.product_width_cm,
    PM.payment_sequential,
    PM.payment_type,
    PM.payment_installments,
    PM.payment_value AS total_payment_value
FROM Consolidated_Orders AS O
INNER JOIN Consolidated_Customers AS C    ON O.customer_id = C.customer_id
INNER JOIN Consolidated_OrderItems AS OI  ON O.order_id = OI.order_id
INNER JOIN Consolidated_Products AS P    ON OI.product_id = P.product_id
INNER JOIN Consolidated_Payments AS PM   ON O.order_id = PM.order_id;
CREATE TABLE personal_analysis.ecommerce_dataset (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_timestamp DATETIME,
    order_estimated_delivery_date DATE,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    seller_id VARCHAR(50),
    item_price DECIMAL(10, 2),
    shipping_charges DECIMAL(10, 2),
    product_category_name VARCHAR(100),
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    payment_sequential INT,
    payment_type VARCHAR(30),
    payment_installments INT,
    total_payment_value DECIMAL(10, 2)
);

-- upload the combined dataset for aggregation and cleaning
LOAD DATA LOCAL INFILE '/Users/michaelessiful/Desktop/Data Analysis/Ecommerce/Ecommerce Order Dataset/ecommerce combine dataset .csv'
INTO TABLE personal_analysis.ecommerce_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Data cleaning and aggregation
-- Deleting all rows containing null entries
CREATE TABLE personal_analysis.ecommerce_clean AS
SELECT *
FROM personal_analysis.ecommerce_dataset
Where
	order_id IS NOT NULL AND
    customer_id IS NOT NULL AND
    seller_id IS NOT NULL AND
    product_category_name IS NOT NULL AND
    item_price IS NOT NULL AND
    shipping_charges IS NOT NULL AND
    total_payment_value IS NOT NULL AND
    order_status IS NOT NULL AND
    order_delivered_timestamp IS NOT NULL AND 
    order_approved_at IS NOT NULL AND
    customer_city IS NOT NULL AND
    customer_state IS NOT NULL;
    
-- converting (making sure the timestamps are in its proper format) 
update personal_analysis.ecommerce_clean
set 
    order_purchase_timestamp = CAST(order_purchase_timestamp AS DATETIME),
    order_approved_at = CAST(order_approved_at AS DATETIME),
    order_delivered_timestamp = CAST(order_delivered_timestamp AS DATETIME),
    -- order_estimated_delivery_date is a DATE type
    order_estimated_delivery_date = CAST(order_estimated_delivery_date AS DATE);

-- creating two columns i.e. delivery_time and late_delivery_flag
ALTER TABLE personal_analysis.ecommerce_clean
add column delivery_time INT,
add column late_delievry_flag TINYINT (1);

UPDATE personal_analysis.ecommerce_clean
SET delivery_time = TIMESTAMPDIFF(HOUR, order_purchase_timestamp, order_delivered_timestamp)
WHERE order_delivered_timestamp IS NOT NULL; -- Only calculate for delivered orders

UPDATE personal_analysis.ecommerce_clean
SET late_delivery_flag = CASE
    WHEN order_delivered_timestamp > order_estimated_delivery_date THEN 1
    -- Handle cases where estimated date is NULL or delivery was on time/early
    ELSE 0
END;
-- Got an error because of a typo so I corrected it
-- Dropped the column with the typo 
ALTER TABLE personal_analysis.ecommerce_clean
DROP COLUMN late_delievry_flag;

-- Added the column with the correct spelling
ALTER TABLE personal_analysis.ecommerce_clean
ADD COLUMN late_delivery_flag TINYINT(1);

-- How many rows are in the clean table?
SELECT COUNT(*) FROM personal_analysis.ecommerce_clean;

-- Confirm the data type of the delivery timestamp column
DESCRIBE personal_analysis.ecommerce_clean order_delivered_timestamp;

-- Calculating delivery_time
UPDATE personal_analysis.ecommerce_clean
SET delivery_time = TIMESTAMPDIFF(HOUR, order_purchase_timestamp, order_delivered_timestamp);


-- Calculating late_delivery_flag (now with the correct column name)
UPDATE personal_analysis.ecommerce_clean
SET late_delivery_flag = CASE
    WHEN order_delivered_timestamp > order_estimated_delivery_date THEN 1
    ELSE 0
END;
-- creating another column approval_time
ALTER TABLE personal_analysis.ecommerce_clean
ADD COLUMN approval_time INT;
UPDATE personal_analysis.ecommerce_clean
SET approval_time = TIMESTAMPDIFF(HOUR, order_purchase_timestamp, order_approved_at)
WHERE order_approved_at IS NOT NULL;

-- some values return zero so i am recalculating in minutes intead of hours
ALTER TABLE personal_analysis.ecommerce_clean
DROP COLUMN approval_time;
ALTER TABLE personal_analysis.ecommerce_clean
ADD COLUMN approval_time INT;
UPDATE personal_analysis.ecommerce_clean
SET approval_time = TIMESTAMPDIFF(MINUTE, order_purchase_timestamp, order_approved_at)
WHERE
    order_approved_at IS NOT NULL
    AND order_purchase_timestamp <= order_approved_at;

-- convert delivery time from hours to days
ALTER TABLE personal_analysis.ecommerce_clean
MODIFY COLUMN delivery_time DECIMAL(6, 2);
UPDATE personal_analysis.ecommerce_clean
SET delivery_time = delivery_time / 24;

-- run the values up to the nearest whole number
UPDATE personal_analysis.ecommerce_clean
SET delivery_time = ROUND(delivery_time);
ALTER TABLE personal_analysis.ecommerce_clean
MODIFY COLUMN delivery_time INT;

-- Data aggregation
-- monthly order count
SELECT
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS purchase_month,
    COUNT(DISTINCT order_id) AS monthly_order_count
FROM
    personal_analysis.ecommerce_clean
GROUP BY
    purchase_month
ORDER BY
    purchase_month;
    
-- Top 10 custimers by number of orders  
SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders
FROM
    personal_analysis.ecommerce_clean
GROUP BY
    customer_id
ORDER BY
    total_orders asc
LIMIT 10;  -- the values returened wrong so I have to recalculate it

--- Top selling products
SELECT
    product_category_name,
    COUNT(DISTINCT order_id) AS total_unique_orders
FROM
    personal_analysis.ecommerce_clean
WHERE
    product_category_name IS NOT NULL
GROUP BY
    product_category_name
ORDER BY
    total_unique_orders DESC
LIMIT 15;

-- delays in delivery
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(late_delivery_flag) AS orders_delivered_late,
    -- Calculates the percentage of late orders
    (SUM(late_delivery_flag) * 100.0) / COUNT(DISTINCT order_id) AS percentage_late
FROM
    personal_analysis.ecommerce_clean
WHERE
    order_status = 'delivered'; -- percentages more than 100 so I'll have recalculate
    
-- product types
SELECT
    COUNT(DISTINCT product_category_name) AS unique_product_category_count
FROM
    personal_analysis.ecommerce_clean;    

-- Averagedays of derlivery by product and customer city
SELECT
    product_category_name,
    customer_city,
   
    ROUND(AVG(delivery_time), 2) AS average_delivery_days
FROM
    personal_analysis.ecommerce_clean
WHERE
    delivery_time IS NOT NULL AND order_status = 'delivered'
GROUP BY
    product_category_name,
    customer_city
HAVING
    COUNT(order_id) >= 10 
ORDER BY
    average_delivery_days DESC
LIMIT 10;     

-- Top cities by order
SELECT
    customer_city,
    COUNT(DISTINCT order_id) AS total_orders
FROM
    personal_analysis.ecommerce_clean
WHERE
    customer_city IS NOT NULL
GROUP BY
    customer_city
ORDER BY
    total_orders DESC
LIMIT 10;

-- Average order value by payment type
SELECT
    payment_type,
    ROUND(SUM(total_payment_value) / COUNT(DISTINCT order_id), 2) AS average_order_value_usd
FROM
    personal_analysis.ecommerce_clean
WHERE
    payment_type IS NOT NULL
GROUP BY
    payment_type
ORDER BY
    average_order_value_usd DESC;