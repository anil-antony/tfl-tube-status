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
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from time import sleep

# Set up Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tube_status_etl")

api_url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"

# Retry configs
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
