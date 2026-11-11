# 🛒 E-Commerce Clickstream Sessionization & Multi-Touch Attribution Engine

An end-to-end, high-performance data analytics and sessionization pipeline built with **DuckDB** and **Python** to process **42.4+ million raw clickstream events (5.67 GB)** from a multi-category online retail platform.

This project implements a custom **30-minute inactivity window sessionization engine**, calculates **session-level cart drop-off rates**, and constructs a **multi-touch revenue attribution model** to distinguish discovery doorway items from closing purchase products.

---

## 📌 Executive Summary

Raw e-commerce event logs capture actions (views, cart additions, purchases) as isolated records without explicit session boundaries. This project constructs an out-of-core analytical engine that: 
1. **Ingests & Standardizes Data**: Parses 42.4M raw CSV logs into DuckDB with optimized data types (`TIMESTAMP`, `BIGINT`, `DOUBLE`).
2. **Models User Sessions**: Dynamically groups clicks into continuous shopping sessions using a 30-minute inactivity threshold. 
3. **Analyzes Conversion & Drop-off**: Quantifies session-level abandonment rates and identifies top drop-off products. 
4. **Attributes Multi-Touch Revenue**: Evaluates entry-point discovery products (`FIRST_VALUE`) versus transaction-closing products (`LAST_VALUE`). 
5. **Scales Out-of-Core**: Utilizes spill-to-disk memory management (`SET temp_directory`) to run window functions across 42M+ rows without memory overhead errors.

---

## 🛠️ Tech Stack & Key Concepts

* **Database Engine**: DuckDB (In-Process / Out-of-Core OLAP Engine) 
* **Language**: Python 3.12, SQL (Advanced Window Functions, CTEs) 
* **Performance Tuning**: Memory-optimized type casting, Spill-to-Disk Out-of-Core Execution (`clickstream_temp.tmp`) 
* **Core Analytics**: Dynamic Sessionization (`LAG()`, `SUM() OVER`), Conversion Funnel Modeling, Revenue Attribution (`FIRST_VALUE`, `LAST_VALUE`)

---

## 📁 Repository Structure 

| File | Description | 
| :--- | :--- | 
| `load_data.py` | Ingests CSV logs into DuckDB with explicit data type casting (`TIMESTAMP`, `BIGINT`, `DOUBLE`). | 
| `01_sessionization.py` | Implements 30-minute idle threshold sessionization logic using SQL window functions. | 
| `02_validation.py` | Audits session timestamps, duration distributions, and sequence validity. | 
| `03_attribution.py` | Evaluates first-touch (discovery) vs. last-touch (closer) products for converting sessions. | 
| `04_cart_dropoff.py` | Analyzes session conversion funnels and product-level cart drop-off rates. | 
| `05_materialize_full.py` | Materializes the 42.4M sessionized dataset to disk using DuckDB spill-to-disk execution. | 
| `run_attribution_full.py` | Executes full-scale multi-touch revenue attribution across pre-computed materialized tables. | 
| `Dataset_Desc.py` | Utility script to verify table availability, schema structure, and data types. |

---

## 🔑 Core SQL Implementation Highlights

### 1. 30-Minute Inactivity Window Sessionization
Calculates timestamp deltas between consecutive events per user to dynamically generate session IDs:

```sql 
WITH event_lags AS ( 
    SELECT user_id, 
    event_time, 
    event_type, 
    product_id, 
    price, 
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
    price, 
    SUM( 
        CASE 
            WHEN previous_event_time IS NULL THEN 1 
            WHEN DATE_DIFF('minute', previous_event_time, event_time) > 30 THEN 1 ELSE 0 END 
    ) OVER ( 
        PARTITION BY user_id O
        ORDER BY event_time 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
    ) AS calculated_session_number 
FROM event_lags;

-- Configure DuckDB spill-to-disk for out-of-core operations 

PRAGMA temp_directory = 'clickstream_temp.tmp'; 

WITH purchasing_sessions AS ( 
    SELECT
        DISTINCT user_id, 
        calculated_session_number AS session_id 
    FROM sessionized_events_full 
    WHERE event_type = 'purchase' 
), 

session_touchpoints AS ( 
    SELECT 
        DISTINCT s.user_id, 
        s.calculated_session_number AS session_id, 
        FIRST_VALUE(s.product_id) OVER ( 
            PARTITION BY s.user_id, s.calculated_session_number 
            ORDER BY s.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
        ) AS first_touch_product, 
        LAST_VALUE(s.product_id) OVER ( 
            PARTITION BY s.user_id, s.calculated_session_number 
            ORDER BY s.event_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 
        ) AS last_touch_product, 
        MAX(
            CASE 
                WHEN s.event_type = 'purchase' THEN s.price ELSE 0 END
            ) OVER ( 
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
    ROUND(SUM(conversion_revenue), 2) AS total_attributed_revenue 
FROM session_touchpoints 
GROUP BY first_touch_product, last_touch_product 
ORDER BY total_attributed_revenue DESC 
LIMIT 10;