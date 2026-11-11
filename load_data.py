import duckdb

#connecting to local database
con = duckdb.connect("clickstream.duckdb") 

con.execute("""
CREATE TABLE IF NOT EXISTS raw_clickstream AS
SELECT 
    event_time::TIMESTAMP AS event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session as original_session_id
FROM read_csv_auto('data/2019-Oct.csv')
LIMIT 5000000

""")

#Run verification summary query
summary = con.execute("""
SELECT 
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS total_users,
    MIN(event_time) AS earliest_event,
    MAX(event_time) AS latest_event
FROM clickstream
""").df()

print("\n--- DATA LOAD COMPLETED ---")
print(summary.to_string(index=False))

con.close()