import pandas as pd
import os
import logging
from datetime import datetime

# Setup logging for bad records
logging.basicConfig(
    filename="bad_records.log", level=logging.INFO, format="%(asctime)s - %(message)s"
)


def validate_and_partition(input_file, output_base_dir):
    total_rows_read = 0
    total_rows_written = 0
    bad_records_count = 0
    chunk_size = 10000  # Requirement: chunks of 10,000

    # Ensure output directory exists
    os.makedirs(output_base_dir, exist_ok=True)

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        total_rows_read += len(chunk)

        # Ensure value column is numeric
        chunk["value"] = pd.to_numeric(chunk["value"], errors="coerce")

        # Validation checks for latitude, longitude, and value
        invalid_lat = ~chunk["latitude"].between(-90, 90)
        invalid_lon = ~chunk["longitude"].between(-180, 180)
        invalid_val = chunk["value"].isna() | (chunk["value"] < 0)

        # Create a mask for bad records and separate them from clean data
        bad_mask = invalid_lat | invalid_lon | invalid_val
        bad_records = chunk[bad_mask]
        clean_data = chunk[~bad_mask].copy()

        # Log invalid rows with reasons
        if not bad_records.empty:
            # Create a DataFrame to hold reasons for invalidity
            reasons_df = pd.DataFrame(
                {
                    "invalid latitude": invalid_lat[bad_mask],
                    "invalid longitude": invalid_lon[bad_mask],
                    "invalid value": invalid_val[bad_mask],
                }
            )
            
            # Combine reasons into a single string for logging
            # For each bad record, join the reasons where the value is True
            # This will create a string like "invalid latitude, invalid value" for records that fail both checks
            # Notice we passed axis=1 to apply the function row-wise
            # The lambda function takes each row of the reasons_df, 
            # checks which columns are True (indicating a failure), 
            # and joins the column names of those True values into a single string.
            reasons_series = reasons_df.apply(
                lambda row: ", ".join(row.index[row].tolist()), axis=1
            )
            for row_id, reason in zip(bad_records["id"], reasons_series):
                logging.info(f"ID {row_id}: {reason}")

            bad_records_count += len(bad_records)

        # Process cleaned data for partitioning
        if not clean_data.empty:
            # Convert timestamp to datetime and extract date for partitioning
            clean_data["timestamp"] = pd.to_datetime(clean_data["timestamp"])
            clean_data["date"] = clean_data["timestamp"].dt.strftime("%Y-%m-%d")

            # Write to Parquet partitioned by date and category
            # Pyarrow handles the folder layout YYYY-MM-DD/category=cat1/ automatically
            clean_data.to_parquet(
                output_base_dir,
                engine="pyarrow",
                partition_cols=["date", "category"],
                index=False,
                max_partitions=2048, 
                max_open_files=1024,
            )
            total_rows_written += len(clean_data)

    # Summary Report
    print(f"--- Ingestion Summary ---")
    print(f"Total rows read: {total_rows_read}")
    print(f"Total rows written: {total_rows_written}")
    print(f"Number of bad records: {bad_records_count}")

    # Warning if bad records exceed 5%
    if total_rows_read > 0 and (bad_records_count / total_rows_read) > 0.05:
        print("WARNING: High data error rate! Bad records exceed 5% of total input.")


if __name__ == "__main__":
    validate_and_partition("datalake/raw/events.csv", "datalake/events/")
