import duckdb

# Connect to disk database containing sessionized_events_full 
con = duckdb.connect("clickstream.duckdb") 

print("--- 🎯 FULL-SCALE MULTI-TOUCH REVENUE ATTRIBUTION ---") 
attribution_df = con.execute(""" 
WITH purchasing_sessions AS ( 
    SELECT DISTINCT 
        user_id, 
        calculated_session_number AS session_id 
    FROM sessionized_events_full 
    WHERE event_type = 'purchase' ), 

    
conversion_paths AS ( 
    SELECT 
        s.user_id, 
        s.calculated_session_number AS session_id, 
        FIRST_VALUE(s.product_id) OVER ( 
            PARTITION BY s.user_id, s.calculated_session_number ORDER BY s.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
        ) AS first_touch_product, 
        LAST_VALUE(s.product_id) OVER ( 
            PARTITION BY s.user_id, s.calculated_session_number ORDER BY s.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
        ) AS last_touch_product, 
        MAX(CASE  WHEN s.event_type = 'purchase' THEN s.price ELSE 0 END) OVER(
            PARTITION BY s.user_id, s.calculated_session_number
        ) AS conversion_revenue 
    FROM sessionized_events_full s 
    INNER JOIN purchasing_sessions p 
    ON s.user_id = p.user_id AND s.calculated_session_number = p.session_id 
) 

SELECT 
    first_touch_product, 
    last_touch_product, 
    COUNT(*) AS total_conversions, 
    ROUND(
        SUM(conversion_revenue), 2
    ) AS total_attributed_revenue 
FROM conversion_paths 
GROUP BY first_touch_product, last_touch_product 
ORDER BY total_attributed_revenue DESC 
LIMIT 10; 

"""
).df() 

print(attribution_df.to_string(index=False))

con.close()