# Exercise 1: Geospatial Event Pipeline
## 1. Project Overview
This project goals is to implement a minimal, production-oriented data pipeline to ingest, validate and analyze a high-volume geospatial events dataset.
1. **Ingestion and Data Quality**: A Python based ETL process that cleans and partitions raw CSV data into Parquet format.
2. **Spatial Analysis**: A SQL-based analytical suite using PostGIS for geospatial insights.

## 2. Architecture and technical choices
- **Python (pandas/pyarrow)**: Chosen for the ingestion script because of its robust handling of chunked data and because it's the imposed programming language. Processing data in chunks of 10,000 ensures the system remains memory-efficient even with datasets exceeding 500k rows.
- **Parquet Storage**: Parquet was selected as the analytical storage format due to its columnar compression, which is highly efficient for the specific queries required (e.g., daily averages and category-based filtering).
- **PostgreSQL with PostGIS**: PostGIS is the gold standard for spatial analytics, providing the geometric functions needed for regional joins and hotspot identification.

## 3. Implementation detail
### Part 1: Ingestion and Validation
The scripts in `validate_and_partition.py` performs the following steps:
- **Validation**: Ensures latitude is within [−90,90], longitude is within [−180,180], and values are numeric and non-negative.
- **Error Logging**: Any row failing validation is logged to bad_records.log with a specific reason for failure (e.g., "invalid latitude").
- **Partitioning**: The output is partitioned by date (YYYY-MM-DD) and category to optimize downstream analytical performance.
- **Quality Check**: Prints a summary of rows processed and triggers a warning if the error rate exceeds 5%.

### Part 2: Analytics & Spatial Queries
The SQL in `analytical_queries.sql` addresses three specific business questions:
- **Daily Averages**: Computes the average value per category for the last 30 days.
- **Regional Event Count**: Uses a spatial join (ST_Intersects) to determine which events fell within predefined regional boundaries in the last 30 days.
- **Hotspot Identification**: Utilizes ST_SnapToGrid to cluster nearby points into grid cells, identifying the top 5 highest-value cells from the last 7 days.

## 4. Assumptions
- The **timestamp** column in `events.csv` is in a standard ISO format (e.g., YYYY-MM-DD HH:MM:SS)
- The `events` and `regions` tables are created into PostgreSQL/PostGIS and loaded with cleaned data from part 1

## 5. Performance Optimization
To ensure the pipeline remains fast as the dataset grows in a production environment, I recommend to add a Generalized Search Tree (**GIST**) index on the **geom** column for both the `events` and `regions` tables. Why ? Without a GIST index some analytical queries will do a sequential scan, making the query extremely slow on high volume of data.