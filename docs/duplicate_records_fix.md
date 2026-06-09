# Duplicate Records Issue in `current_weather`
## Date: 2026-06-09

---

## Problem

During validation of the Smart City data pipeline, we noticed that the `current_weather` data contained multiple records for the same city within the same hour.

We first checked the data by grouping records by `city`, `country`, and hourly `observed_at`. The result showed that some cities had 2 to 4 records within the same hour.

**Example:**
```
Amsterdam | 2026-06-09 08:00 | 2 records
Berlin    | 2026-06-09 08:00 | 2 records
London    | 2026-06-09 08:00 | 2 records
Madrid    | 2026-06-09 08:00 | 2 records
```

After checking the actual records, we found that not all of them were exact duplicates. Some records had different `observed_at`, temperature, and humidity values.

**Example:**
```
Amsterdam | 08:10:43 | temp 15.80 | humidity 66
Amsterdam | 08:40:25 | temp 16.18 | humidity 64
```

This means that some records were not true duplicates, but multiple weather observations within the same hour.

---

## Root Cause

The main cause of the issue was that two ingestion sources were running at the same time:

```
1. The Python script ingest.py, scheduled through an Airflow DAG
2. Airbyte connections, which were also syncing data hourly
```

Both ingestion sources were writing into the same PostgreSQL raw tables in the `airbyte_raw` schema.

So the pipeline was previously working like this:

```
OpenWeather / TomTom APIs
        ↓
ingest.py through Airflow
        ↓
PostgreSQL airbyte_raw

AND at the same time:

OpenWeather / TomTom APIs
        ↓
Airbyte connections
        ↓
PostgreSQL airbyte_raw
```

Because of this, similar records were inserted multiple times into the same table.

---

## What We Changed

To prevent duplicate ingestion, we disabled the Airbyte connections using the toggle OFF option in the Airbyte UI (http://localhost:8000).

We kept only one ingestion source active: `ingest.py` through Airflow.

**Updated architecture:**
```
OpenWeather / TomTom APIs
        ↓
ingest.py
        ↓
Airflow DAG runs ingest.py
        ↓
PostgreSQL airbyte_raw schema
        ↓
dbt staging views
        ↓
intermediate / marts layers (in progress)
```

---

## How We Verified the Fix

To check what was inserted during the 11:00 run, we used `extracted_at`, because:
- `observed_at` = the time when the API says the weather measurement was observed
- `extracted_at` = the time when our pipeline extracted/inserted the record

**Verification query:**
```sql
SELECT
    city,
    country,
    DATE_TRUNC('hour', extracted_at) AS extracted_hour,
    COUNT(*) AS records_count
FROM staging.stg_current_weather
WHERE DATE_TRUNC('hour', extracted_at) = TIMESTAMP '2026-06-09 11:00:00'
GROUP BY
    city,
    country,
    DATE_TRUNC('hour', extracted_at)
ORDER BY city;
```

**Result:**
```
Amsterdam   NL   2026-06-09 11:00   1
Berlin      DE   2026-06-09 11:00   1
London      GB   2026-06-09 11:00   1
Madrid      ES   2026-06-09 11:00   1
```

This confirmed that after disabling Airbyte, the 11:00 ingestion produced only one record per city.

---

## Conclusion

The duplicate record issue was caused by two ingestion mechanisms running at the same time: Airbyte syncs and the Airflow `ingest.py` process. Both were writing to the same raw tables.

After disabling the Airbyte connections, `ingest.py` remained the only active ingestion source. We verified the 11:00 ingestion run using `extracted_at` and confirmed that only one record per city was inserted.

---

## What Was Solved

```
✅ Identified possible duplicate records by city and hour
✅ Checked that some records were multiple observations within the same hour
✅ Found the root cause: two active ingestion sources
✅ Disabled Airbyte connections
✅ Kept ingest.py as the single ingestion source
✅ Verified that the 11:00 ingestion had only one record per city
✅ Confirmed that new records are no longer duplicated
```

---

## Recommended Next Step

Even though the issue is fixed for new records, it is still recommended to add deduplication logic in the dbt intermediate layer.

**Suggested model:** `int_current_weather_deduped.sql`

This model would keep one record per city per hour (the latest one based on `observed_at` and `extracted_at`):

```sql
WITH ranked AS (
    SELECT
        *,
        DATE_TRUNC('hour', observed_at) AS observed_hour,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, DATE_TRUNC('hour', observed_at)
            ORDER BY observed_at DESC, extracted_at DESC
        ) AS rn
    FROM staging.stg_current_weather
)

SELECT *
FROM ranked
WHERE rn = 1;
```

This approach does not delete raw history. Instead, it creates a clean intermediate model for analytics.
