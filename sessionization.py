import duckdb

#Connect to the duckDb database created by load_data.py 
con= duckdb.connect("clickstream.duckdb")

#Multi-step CTE query to calculate idle gaps and construct dynamic session ids
sessionization_query = """
WITH events_with_lag AS (
    ---Step 1: Get timestamp of previous event for each user using LAG() window function
    SELECT 
        user_id,
        event_time,
        event_type,
        product_id,
        price,
        original_session_id,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS previous_event_time
        FROM raw_clickstream
),

session_flags AS (
    ---Step 2: Flag a new session ('1') if idle time> 30 minutes or first event
    SELECT 
        *,
        DATE_DIFF('minute', previous_event_time, event_time) AS idle_minutes,
        CASE 
            WHEN previous_event_time is NULL THEN 1
            WHEN DATE_DIFF('minute', previous_event_time, event_time) > 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM events_with_lag
),

session_indexing AS (
    ---Step 3: Create a cumulative session index for each user based on the new session
    SELECT 
        user_id,
        event_time,
        event_type,
        idle_minutes,
        original_session_id,
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS calculated_session_number
    FROM session_flags
)

-- Inspect session transition for users
SELECT 
    user_id,
    event_time,
    event_type,
    idle_minutes,
    original_session_id,
    calculated_session_number
FROM session_indexing
ORDER BY user_id, event_time
LIMIT 30;

"""
print("Executing SQL sessionization query...")         
df_results = con.execute(sessionization_query).df()

print("\\n--- SESSIONIZATION ENGINE RESULTS ---") 
print(df_results.to_string(index=False))

con.close()
