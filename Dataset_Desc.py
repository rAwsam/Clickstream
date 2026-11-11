import duckdb

#connecting to local database
con = duckdb.connect("clickstream.duckdb")

# Step 1: List all tables in this database file 
print("\n--- 📁 TABLES FOUND IN CLICKSTREAM.DUCKDB ---") 
tables_df = con.execute("SHOW TABLES;").df() 
print(tables_df) 

# Step 2: Check if raw_clickstream exists 
if not tables_df.empty and 'raw_clickstream' in tables_df['name'].values: 
    print("\n--- 📋 SCHEMA FOR raw_clickstream ---") 
    schema_df = con.execute("DESCRIBE raw_clickstream;").df() 
    print(schema_df[["column_name", "column_type"]]) 
else: 
    print("\n 'raw_clickstream' table was NOT found in this database file.") 
    
con.close()