create database amazon;
use amazon;

-- Q1 
SELECT
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.price_per_unit) AS total_sales_value
FROM order_items AS oi
JOIN products AS p
    ON oi.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name
ORDER BY
    total_sales_value DESC
LIMIT 10;

-- Q2
WITH category_sales AS (
    SELECT
        c.category_name,
        SUM(oi.quantity * oi.price_per_unit) AS category_revenue
    FROM order_items AS oi
    JOIN products AS p
        ON oi.product_id = p.product_id
    JOIN category AS c
        ON p.category_id = c.category_id
    GROUP BY
        c.category_name
)
SELECT
    category_name,
    category_revenue,
    ROUND(
        100.0 * category_revenue
        / SUM(category_revenue) OVER (),
        2
    ) AS revenue_percentage
FROM category_sales
ORDER BY
    category_revenue DESC;

-- Q3
WITH order_totals AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * oi.price_per_unit) AS order_value
    FROM order_items AS oi
    GROUP BY
        oi.order_id
),
customer_orders AS (
    SELECT
        o.customer_id,
        ot.order_value
    FROM orders AS o
    JOIN order_totals AS ot
        ON o.order_id = ot.order_id
)
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(*) AS total_orders,
    ROUND(AVG(order_value), 2) AS avg_order_value
FROM customer_orders AS co
JOIN customers AS c
    ON co.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name
HAVING
    COUNT(*) > 5
ORDER BY
    avg_order_value DESC;
    
-- Q4
WITH monthly_sales AS (
  SELECT
    DATE_FORMAT(o.order_date, '%Y-%m-01') AS month_start,
    SUM(oi.quantity * oi.price_per_unit) AS month_sales
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  WHERE o.order_date >= (
    SELECT DATE_SUB(MAX(order_date), INTERVAL 1 YEAR) FROM orders
  )
  GROUP BY DATE_FORMAT(o.order_date, '%Y-%m-01')
)
SELECT
  cur.month_start AS month,
  cur.month_sales AS current_month_sales,
  prev.month_sales AS previous_month_sales
FROM monthly_sales cur
LEFT JOIN monthly_sales prev
  ON prev.month_start = DATE_FORMAT(
       DATE_SUB(cur.month_start, INTERVAL 1 MONTH),
       '%Y-%m-01'
     )
ORDER BY cur.month_start;

-- Q5
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.state
FROM customers AS c
LEFT JOIN orders AS o
    ON o.customer_id = c.customer_id
WHERE
    o.order_id IS NULL;
    
-- Q6
with state_category_sales as (
	select
    c.state,
    cat.category_name,
    sum(oi.quantity * oi.price_per_unit) as total_sales
    from customers as c
    join orders as o on o.customer_id = c.customer_id
    join order_items as oi on oi.order_id = o.order_id
    join products as p on p.product_id = oi.product_id
    join category as cat on cat.category_id = p.category_id
    group by c.state, cat.category_name
),
ranked as(
	select
    state,
    category_name,
    total_sales,
    row_number() over(partition by state order by total_sales asc) as rn
    from state_category_sales
)
select
state,
category_name as least_selling_category,
total_sales
from ranked
where rn = 1
order by state;


-- Q7
with cltv as (
	select
	customer_id,
	full_name,
	round(coalesce(sum(revenue), 0), 2) as CLTV
	from (
		select
		c.customer_id,
		concat(lower(c.first_name)," ",lower(c.last_name)) as full_name,
		o.order_id,
        (oi.quantity * oi.price_per_unit) as revenue
		from customers c
		left join orders o on c.customer_id = o.customer_id and o.order_status = 'Completed'
		left join order_items oi on o.order_id = oi.order_id
	) t
	group by customer_id, full_name
)
select
dense_rank() over(order by CLTV desc) as cltv_rank,
customer_id,
full_name,
CLTV
from cltv;


-- Q8
set @stock_threshold = 10;
select
p.product_id,
p.product_name,
i.stock,
i.warehouse_id,
i.last_stock_date
from products p
join inventory i on p.product_id = i.product_id
where i.stock<@stock_threshold;



-- Q9
SELECT
    payment_status,
    COUNT(*) AS payment_count,
    ROUND(
        COUNT(*) * 100.0 / sum(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM payments
GROUP BY payment_status;

select 
delivery_status,
count(order_id) as no_of_orders
from shipping
group by delivery_status;

select
order_status,
count(order_id) as no_of_orders
from orders
group by order_status;

-- Q10
SELECT
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(
        CASE
            WHEN s.return_date IS NOT NULL
            THEN oi.quantity
            ELSE 0
        END
    ) AS returned_units,
    ROUND(
        SUM(
            CASE
                WHEN s.return_date IS NOT NULL
                THEN oi.quantity
                ELSE 0
            END
        ) * 100.0 / SUM(oi.quantity),
        2
    ) AS return_rate_percentage
FROM products p
JOIN order_items oi
    ON p.product_id = oi.product_id
JOIN orders o
    ON oi.order_id = o.order_id
LEFT JOIN shipping s
    ON o.order_id = s.order_id
GROUP BY p.product_id, p.product_name
ORDER BY returned_units DESC
LIMIT 10;

SELECT 
    DATE_FORMAT(o.order_date, '%Y-%m') AS month,
    SUM(oi.quantity * oi.price_per_unit) AS monthly_sales
FROM orders o
JOIN order_items oi 
    ON o.order_id = oi.order_id
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
GROUP BY month
ORDER BY month;
