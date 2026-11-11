import duckdb

#connecting to local database
con = duckdb.connect("clickstream.duckdb")

cart_dropoff_query = """
WITH event_lags AS (
    SELECT
        user_id,
        event_time,
        event_type,
        product_id,
        brand,
        price,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_event_time
    FROM raw_clickstream
),

sessionized_events AS (
    SELECT
        user_id,
        event_time,
        event_type,
        product_id,
        brand,
        price,
        SUM(
            CASE
                WHEN previous_event_time IS NULL THEN 1 
                WHEN DATE_DIFF('minute', previous_event_time, event_time) > 30 THEN 1 
                ELSE 0
            END
        ) OVER (
            PARTITION BY user_id 
            ORDER BY event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM event_lags
),

session_funnel AS (
    --Step 2 : Flag funnel stage progression per session
    SELECT
        user_id,
        session_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS added_to_cart,
        Max(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM sessionized_events
    GROUP BY user_id, session_id
)

--Output 1 :Session-level conversion funnel and drop-off rates
SELECT
    COUNT(*) AS total_sessions,
    SUM(viewed) AS sessions_with_view,
    SUM(added_to_cart) AS sessions_with_add_to_cart,
    SUM(purchased) AS sessions_with_purchase,
    SUM(CASE 
            WHEN added_to_cart = 1 AND purchased = 0 THEN 1     
            ELSE 0 
        END) AS Abandoned_cart_sessions,
    ROUND(
        (SUM
            (CASE   
                WHEN added_to_cart = 1 AND purchased = 0 THEN 1 
                ELSE 0 
            END) :: double/ NULLIF(SUM(added_to_cart), 0))*100, 2) AS cart_abandonment_rate_percentage
FROM session_funnel;
"""

print("Executing Session-level Cart Drop-off Analysis Query...")
df_funnel = con.execute(cart_dropoff_query).df()

print("\n--- SESSION-LEVEL CONVERSION FUNNEL ---")
print(df_funnel.to_string(index=False))

#Product level high abandonment item analysis
product_dropoff_query = """
with sessionized_events AS (
    SELECT
        user_id,
        event_time,
        event_type,
        product_id,
        brand,
        price
    FROM raw_clickstream
)

SELECT
    product_id,
    brand,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS total_add_to_cart,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) - SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_abandoned,
    ROUND(
        (SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) - SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)) :: double / NULLIF(SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END), 0) * 100, 2
    ) AS product_abandonment_rate_percentage
FROM sessionized_events
GROUP BY product_id, brand
HAVING SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) >= 10
ORDER BY total_abandoned DESC
LIMIT 20;

"""

df_products = con.execute(product_dropoff_query).df()

print("\n--- 🏷️ TOP 10 PRODUCTS WITH HIGHEST CART ABANDONMENT ---") 
print(df_products.to_string(index=False)) 

con.close()
