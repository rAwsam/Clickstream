import duckdb

#connecting to local database
con = duckdb.connect("clickstream.duckdb")

attribution_query = """
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
    --Step 2: Assigining session ids based on idle time > 30 minutes
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

purchasing_events AS (
    --Step 3: Filter only purchasing events for attribution analysis
    SELECT DISTINCT 
        user_id,
        session_id
    FROM sessionized_events
    WHERE event_type = 'purchase'
),

conversion_paths AS (
    --Step 4: Extract First touch and Last touch products per converting session
    SELECT
        s.user_id,
        s.session_id,
        s.event_time,
        s.event_type,
        s.product_id,
        s.price,
        FIRST_VALUE(s.product_id) OVER (
            PARTITION BY s.user_id, s.session_id 
            ORDER BY s.event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_product,
        LAST_VALUE(s.product_id) OVER (
            PARTITION BY s.user_id, s.session_id 
            ORDER BY s.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_product
    FROM sessionized_events s
    Inner JOIN purchasing_events p
        ON s.user_id = p.user_id AND s.session_id = p.session_id
)

--Output: Attributed revenue by conversion touchpoint (first touch and last touch)
SELECT
    user_id,
    session_id,
    first_touch_product,
    last_touch_product,
    MAX(CASE WHEN event_type = 'purchase' THEN price END) AS conversion_revenue
FROM conversion_paths
GROUP BY user_id, session_id, first_touch_product, last_touch_product
LIMIT 20;
"""


print("Executing Multi-Touch Attribution Engine...") 
duckdb.df_attribution = con.execute(attribution_query).df() 

print("\n--- 🎯 MULTI-TOUCH ATTRIBUTION RESULTS ---") 
print(duckdb.df_attribution.to_string(index=False)) 

con.close()