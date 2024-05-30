# TFL Tube Line Status ETL Process

## Summary

This notebook fetches the latest Tube line reports from the TFL Open API and processes the data to store it in Databricks SQL Table. The notebook performs the following steps:

1. Fetches the latest Tube status from the TFL Open API.
2. Ingests the raw data into a Bronze Delta Lake table.
3. Cleans, de-duplicates, and transforms the raw data, and then loads it into a Silver Delta Lake table.
4. Merges the latest Tube status data into a final table, ensuring no duplicate entries based on `line` and `current_timestamp`.

## Step-by-Step Explanation

### Step 1: Fetch Latest Tube Status from TFL API

- **Description:** This step fetches the latest Tube status from the TFL Open API using the `requests` library.
- **Error Handling:** Standard error handling is implemented to catch and log any exceptions that may occur during the process. It includes retry logic with a maximum of 3 retries and a delay of 5 seconds between retries for server errors (500, 502, 503, 504)
- **Output:** The fetched data is stored in a variable `tube_status_data`.

### Code:
```python
import requests
import json 
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from time import sleep

#Set up Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tube_status_etl")

api_url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"

#retry configs
max_retries = 3
retry_delay = 5

tube_status_data = None

for attempt in range(max_retries):
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        tube_status_data = response.json()
        current_timestamp_value = current_timestamp()
        logging.info("Latest Tube Status fetched successfully from the TFL API.")
        break
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from TFL API: {e}")
        if attempt < max_retries - 1:
            status_code = response.status_code if response else None
            if status_code in [500, 502, 503, 504]: 
                logging.info(f"Server error {status_code}. Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
            else:
                logging.error("Non-retryable error occurred. Unable to fetch Tube status data.")
                break
        else:
            logging.error("Max retries exceeded. Unable to fetch Tube status data.")
            break
```
### Step 2: Ingest Raw Data into Bronze Table

- **Description:** This step creates a DataFrame from the fetched data and writes it to a Bronze Delta Lake table.
- **Output:** Raw data is stored in a table named `bronze_tube_status`.
### Code:
```python
if tube_status_data:
    spark = SparkSession.builder.appName("TubeLineStatus").getOrCreate()
    bronze_df = spark.createDataFrame([(json.dumps(tube_status_data),)], ["raw_data"])
    bronze_df = bronze_df.withColumn("current_timestamp", current_timestamp_value)

    bronze_table_name = "bronze_tube_status"
    try:
        bronze_df.write.mode("overwrite").format("delta").saveAsTable(bronze_table_name)
        logger.info(f"Raw data ingested to Bronze Delta Lake table '{bronze_table_name}' successfully.")
    except Exception as e:
        logger.error(f"An error occurred while ingesting raw data to Bronze Delta Lake table '{bronze_table_name}': {e}")
else:
    logger.warning("No data fetched from TFL API. Skipping data ingestion.")
```
### Step 3: Clean, De-Dup, Transform Raw Data and Load Into Silver Table

- **Description:** This step processes the raw data by parsing the JSON, extracting relevant fields, and performing data cleaning and de-duplication. The cleaned data is then written to a Silver Delta Lake table.
- **Output:** Processed data is stored in a table named `silver_tube_status`.
### Code:
```python

if tube_status_data:
    silver_schema = ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("lineStatuses", ArrayType(StructType([
            StructField("statusSeverityDescription", StringType(), True),
            StructField("reason", StringType(), True)
        ])), True)
    ]))

    #reading relevant details from raw data
    silver_df = (spark.table("bronze_tube_status")
                 .withColumn("parsed_data", from_json(col("raw_data"), silver_schema))
                 .selectExpr("current_timestamp", "inline(parsed_data)"))

    silver_df = silver_df.select(
        col("current_timestamp").cast(TimestampType()),
        col("name").alias("line"),
        col("lineStatuses")[0]["statusSeverityDescription"].alias("status"),
        col("lineStatuses")[0]["reason"].alias("disruption_reason")
    )

    #De-Dup
    silver_df = silver_df.dropDuplicates()

    #Missing Values Handling
    silver_df = silver_df.filter(col("current_timestamp").isNotNull() & col("line").isNotNull())
    silver_df = silver_df.withColumn("status", when(col("status").isNull(), "unknown").otherwise(col("status")))
    silver_df = silver_df.withColumn("disruption_reason", 
                                    when(col("status") == "Good Service", "No disruption")
                                    .otherwise(when(col("disruption_reason").isNull(), "unknown")
                                                .otherwise(col("disruption_reason"))))

    silver_table_name = "silver_tube_status"
    try:
        silver_df.write.mode("overwrite").format("delta").saveAsTable(silver_table_name)
        logger.info(f"Processed data loaded to Silver Delta Lake table '{silver_table_name}' successfully.")
    except Exception as e:
        logger.error(f"An error occurred while loading processed data to Silver Delta Lake table '{silver_table_name}': {e}")
else:
    logger.warning("No data fetched from the API. Skipping data transformation and loading.")
```
### Step 4: Load the Latest Tube Status into Final Table

- **Description:** This step merges the latest processed data into the final table `tube_line_status`. It ensures no duplicate entries by matching on `line` and `current_timestamp`.
- **Output:** Latest tube status data is stored in the table `tube_line_status`.
### Code:
```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS tube_line_status (
    current_timestamp TIMESTAMP,
    line STRING,
    status STRING,
    disruption_reason STRING)
""")
if tube_status_data:
    try:
        spark.sql("""
            MERGE INTO tube_line_status AS target
            USING silver_tube_status AS source
            ON target.line = source.line AND target.current_timestamp = source.current_timestamp
            WHEN NOT MATCHED THEN
                INSERT (current_timestamp, line, status, disruption_reason)
                VALUES (source.current_timestamp, source.line, source.status, source.disruption_reason)
        """)
        logger.info("Latest Tube Status loaded into final table 'tube_line_status' successfully.")
    except Exception as e:
        logger.error("An error occurred while loading data into the final table 'tube_line_status':", e)
else:
    logger.warning("No data fetched from the API. Skipping final table update.")
```
