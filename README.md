## E-commerce Data Cleaning and Analytical Feature Engineering

This document outlines the step-by-step SQL process I followed to transform raw e-commerce data into a clean, single analytical table (personal_analysis.ecommerce_clean), and subsequently, the aggregation queries I used to extract key business insights.

## 1.0 Initial Data Preparation

The first step was consolidating the necessary data into a single source table, personal_analysis.ecommerce_dataset, and ensuring all critical timestamp fields were correctly formatted as DATETIME objects for time-series analysis.

## 1.1 Timestamp Type Casting

I used the CAST function to convert string-based timestamp columns to the necessary DATETIME type, which is crucial for all subsequent time-based calculations.

	UPDATE personal_analysis.ecommerce_dataset
	SET
	    order_purchase_timestamp = CAST(order_purchase_timestamp AS DATETIME),
	    order_approved_at = CAST(order_approved_at AS DATETIME),
	    order_delivered_timestamp = CAST(order_delivered_timestamp AS DATETIME),
	    order_estimated_delivery_date = CAST(order_estimated_delivery_date AS DATE);


##	1.2 Creating the Clean Master Table

To ensure the analysis was reliable, I created a new table, personal_analysis.ecommerce_clean, by filtering out all rows with critical missing values related to product identity, payment, and final delivery status.

	CREATE TABLE personal_analysis.ecommerce_clean AS
	SELECT *
	FROM personal_analysis.ecommerce_dataset
	WHERE
	    order_id IS NOT NULL AND
	    customer_id IS NOT NULL AND
	    seller_id IS NOT NULL AND
	    product_category_name IS NOT NULL AND
	    item_price IS NOT NULL AND
	    shipping_charges IS NOT NULL AND
	    total_payment_value IS NOT NULL AND
	    order_status IS NOT NULL AND
	    order_approved_at IS NOT NULL AND
	    order_delivered_timestamp IS NOT NULL;


##	1.3 Data Standardization (Lowercasing)

I standardized all categorical text fields (like city and product name) to lowercase to prevent duplicates caused by case sensitivity (e.g., 'Toys' vs. 'toys').

	UPDATE personal_analysis.ecommerce_clean
	SET
	    order_status = LOWER(order_status),
	    customer_city = LOWER(customer_city),
	    customer_state = LOWER(customer_state),
	    product_category_name = LOWER(product_category_name),
	    payment_type = LOWER(payment_type);


##	2. Feature Engineering

This stage involved creating new, analytically powerful columns based on existing data to extract insights into delivery performance and processing efficiency.

##	2.1 Delivery Performance Columns

I added two columns to measure delivery efficiency: actual delivery duration and a flag for lateness.

delivery_time (Days): Calculated the duration between purchase and delivery, rounded to the nearest whole day.

late_delivery_flag (0 or 1): A binary flag set to 1 if the delivery timestamp exceeded the estimated date.

	-- Add Columns
	ALTER TABLE personal_analysis.ecommerce_clean
	ADD COLUMN delivery_time INT,
	ADD COLUMN late_delivery_flag TINYINT(1);
	
	-- Populate delivery_time (Initial calc in hours, converted to days, and rounded)
	UPDATE personal_analysis.ecommerce_clean
	SET delivery_time = ROUND(TIMESTAMPDIFF(HOUR, order_purchase_timestamp, order_delivered_timestamp) / 24);
	
	-- Populate late_delivery_flag
	UPDATE personal_analysis.ecommerce_clean
	SET late_delivery_flag = CASE
	    WHEN order_delivered_timestamp > order_estimated_delivery_date THEN 1
	    ELSE 0
	END;


##	2.2 Approval Processing Time

I calculated the time elapsed between the order being placed and officially approved. Critically, I used MINUTES instead of hours to capture micro-delays that were being hidden by rounding down when calculating in hours.

	-- Add Column
	ALTER TABLE personal_analysis.ecommerce_clean
	ADD COLUMN approval_time INT;
	
	-- Populate approval_time (in minutes)
	UPDATE personal_analysis.ecommerce_clean
	SET approval_time = TIMESTAMPDIFF(MINUTE, order_purchase_timestamp, order_approved_at)
	WHERE
	    order_approved_at IS NOT NULL
	    AND order_purchase_timestamp <= order_approved_at;


##	3. Data Aggregation Queries

With the clean table prepared, I ran several aggregation queries to generate the final analytical reports.

##	3.1 Monthly Order Count

	SELECT DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS purchase_month, COUNT(DISTINCT order_id) AS monthly_order_count
	FROM personal_analysis.ecommerce_clean
	GROUP BY purchase_month ORDER BY purchase_month;


## 3.2 Top 10 Customers by Number of Orders

	SELECT customer_id, COUNT(DISTINCT order_id) AS total_orders
	FROM personal_analysis.ecommerce_clean
	GROUP BY customer_id ORDER BY total_orders DESC LIMIT 10;


##	3.3 Top-Selling Products (by unique order count)

	SELECT product_category_name, COUNT(DISTINCT order_id) AS total_unique_orders
	FROM personal_analysis.ecommerce_clean
	WHERE product_category_name IS NOT NULL
	GROUP BY product_category_name ORDER BY total_unique_orders DESC LIMIT 10;


##	3.4 Delays in Deliveries Summary

	SELECT COUNT(DISTINCT order_id) AS total_orders, SUM(late_delivery_flag) AS orders_delivered_late,
	(SUM(late_delivery_flag) * 100.0) / COUNT(DISTINCT order_id) AS percentage_late
	FROM personal_analysis.ecommerce_clean
	WHERE order_status = 'delivered';


##	3.5 Average Days of Delivery by Product Type and Customer City

	SELECT product_category_name, customer_city, ROUND(AVG(delivery_time), 2) AS average_delivery_days
	FROM personal_analysis.ecommerce_clean
	WHERE delivery_time IS NOT NULL AND order_status = 'delivered'
	GROUP BY product_category_name, customer_city
	HAVING COUNT(order_id) >= 10
	ORDER BY average_delivery_days DESC LIMIT 10;


##	3.6 Top Cities by Orders

	SELECT customer_city, COUNT(DISTINCT order_id) AS total_orders
	FROM personal_analysis.ecommerce_clean
	WHERE customer_city IS NOT NULL
	GROUP BY customer_city ORDER BY total_orders DESC LIMIT 10;


##	3.7 Average Order Value by Payment Type

	SELECT payment_type, ROUND(SUM(total_payment_value) / COUNT(DISTINCT order_id), 2) AS average_order_value_usd
	FROM personal_analysis.ecommerce_clean
	WHERE payment_type IS NOT NULL
	GROUP BY payment_type ORDER BY average_order_value_usd DESC;


##	4. Key Recommendations

Based on the analysis performed using the above queries, I derived the following actionable business recommendations:

**Recommendation 1:** Targeted Logistical Review
The late_delivery_flag identified a clear area of concern (approximately 8% of deliveries are late). I recommend a focused review of the results from Query 3.5 (Average Days of Delivery by Product Type and Customer City). This will pinpoint the exact product/city combinations with the highest average delivery times, allowing the logistics team to prioritize solutions for specific high-friction routes.

**Recommendation 2: **Optimize Payment Strategy for AOV
The analysis of Average Order Value (AOV) by payment type (Query 3.7) revealed significant differences in spending habits. I recommend testing promotional strategies specifically targeting the payment type with the highest AOV (e.g., Credit Card users) to further incentivize larger transactions, and conversely, investigate strategies to raise the AOV for lower-value payment types (e.g., Wallet/Voucher users).

**Recommendation 3:** Investigate Instant Approval Time Gaps
The new approval_time (in minutes) column should be analyzed further. I recommend segmenting orders where approval_time is greater than 5 minutes. This segment represents orders that required manual review or faced payment gateway friction, providing the operations team with a direct metric to optimize payment processing flow and reduce customer waiting time post-purchase.

##	5. Conclusion

By completing the robust cleaning, feature engineering, and aggregation steps using SQL, I successfully transformed the raw dataset into a powerful resource for strategic business analysis. The resulting ecommerce_clean table is a highly valuable asset for ongoing performance monitoring and decision-making.
