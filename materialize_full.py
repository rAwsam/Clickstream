import duckdb
import time

print("Starting full scale production engine(42M+ rows).....")
start_time = time.time()

#Connecting to local database
con = duckdb.connect("clickstream.duckdb")

#Enable out-of-core execution (spill to disk) for large datasets to prevent OutOfMemory excdeption
con.execute("SET temp_directory= 'clickstream_temp,tmp';")

#Step 1:Check if raw_clickstream table exists
raw_count = con.execute("SELECT COUNT(*) FROM raw_clickstream").fetchone()[0]

if raw_count> 0:
    print(f"\n[1/3]  Found {raw_count:,} records in raw_clickstream table. Skipping ingestion step...")
else:
    print("\n[1/3]  Ingesting full 5.67GB dataset into raw_clickstream table in DuckDB...")
    con.execute("""
    CREATE OR REPLACE TABLE raw_clickstream AS
    SELECT 
        event_time::TIMESTAMP AS event_time,
        event_type,
        product_id::BIGINT AS product_id,
        category_id::BIGINT AS category_id,
        category_code,
        brand,
        price::DOUBLE AS price,
        user_id::BIGINT AS user_id,
        user_session as original_session_id
    FROM read_csv_auto('data/2019-Oct.csv')
    """)

    raw_count = con.execute("SELECT COUNT(*) AS total_events FROM raw_clickstream").fetchone()[0]
    print(f"-->Ingested {raw_count:,} total raw clickstream records!")

#Step 2: Materialize the 30 minute sessionization engine directly to disk

print("\n[2/3]  Materializing 30-minute sessionization engine across full dataset...")
con.execute("""
CREATE OR REPLACE TABLE sessionized_events_full AS
WITH events_with_lag AS (
    SELECT 
        user_id,
        event_time,
        event_type,
        product_id,
        brand,
        category_id,
        category_code,
        price,
        original_session_id,
        LAG(event_time) OVER (
            PARTITION BY user_id 
            ORDER BY event_time
        ) AS previous_event_time
    FROM raw_clickstream
)

SELECT
    user_id,
    event_time,
    event_type,
    product_id,
    brand,
    category_id,
    category_code,
    price,
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
FROM events_with_lag;
""")

sessionized_count = con.execute("SELECT COUNT(*) FROM sessionized_events_full;").fetchone()[0]
print(f"---> Succesfully materialised {sessionized_count:,} sessionized events to disk!")

#Step 3: Run dataset wide production summary statistics
print("\n[3/3]  Generating full scale production metrics...")
summary_stats = con.execute("""
SELECT
    COUNT(DISTINCT user_id) AS total_unique_users,
    COUNT(DISTINCT (user_id || '-' || calculated_session_number::VARCHAR)) AS total_calculated_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS total_carts,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views
FROM sessionized_events_full;
""").df()

elapsed = time.time() - start_time

print("\n================================================")
print("\n--- FULL SCALE PRODUCTION ENGINE COMPLETE ---")
print("\n===================================================")
print(summary_stats.to_string(index=False))
print(f"\n--- Total Execution Time: {elapsed:.2f} seconds {elapsed/60:.2f} minutes ---")

con.close()