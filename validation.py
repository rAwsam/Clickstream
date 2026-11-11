import duckdb

#connecting to local database
con = duckdb.connect("clickstream.duckdb")  

validation_query = """
WITH session_flags AS (
    SELECT
        user_id,
        event_time,
        original_session_id,
        LAG(event_time) OVER (
            PARTITION BY user_id 
            ORDER BY event_time
        ) AS previous_event_time
    FROM raw_clickstream
),

session_indexing AS (
    SELECT
        user_id,
        event_time,
        original_session_id,
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
        ) AS calculated_session_number
    FROM session_flags
)

--Grouping by user and calculated session to audit metrics
SELECT 
    user_id,
    calculated_session_number,
    COUNT(*) AS total_events_in_session,
    COUNT(DISTINCT original_session_id) AS distinct_original_sessions,
    MIN(event_time) AS session_start_time,
    MAX(event_time) AS session_end_time,
    DATE_DIFF('minute', MIN(event_time), MAX(event_time)) AS session_duration_minutes
FROM session_indexing
GROUP BY user_id, calculated_session_number
ORDER BY user_id, calculated_session_number
LIMIT 20;
"""

print("Executing Session Validation Audit...")
df_validation = con.execute(validation_query).df()

print("\n--- SESSION VALIDATION AUDIT RESULTS ---")
print(df_validation.to_string(index=False))

con.close()