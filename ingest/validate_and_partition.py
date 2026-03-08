import pandas as pd
import os
import logging
import time

# Setup logging for bad records
logging.basicConfig(
    filename="bad_records.log",
    level=logging.WARNING,
    format="%(asctime)s - %(message)s",
)

BAD_RECORD_THRESHOLD = 0.05  # 5% threshold for bad records
CHUNK_SIZE = 10000  # Number of rows to process at a time


def validate_and_partition(input_file, output_base_dir):
    start_time = time.time()
    total_rows_read = 0
    total_rows_written = 0
    bad_records_count = 0

    # Ensure output directory exists
    os.makedirs(output_base_dir, exist_ok=True)

    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE):
        total_rows_read += len(chunk)

        # Ensure latitude and longitude are numeric
        chunk["latitude"] = pd.to_numeric(chunk["latitude"], errors="coerce")
        chunk["longitude"] = pd.to_numeric(chunk["longitude"], errors="coerce")

        # Ensure value column is numeric
        chunk["value"] = pd.to_numeric(chunk["value"], errors="coerce")

        # Validation checks for latitude, longitude, and value
        invalid_lat = ~chunk["latitude"].between(-90, 90) | chunk["latitude"].isna()
        invalid_lon = ~chunk["longitude"].between(-180, 180) | chunk["longitude"].isna()
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
                logging.warning(f"ID {row_id}: {reason}")

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
                max_open_files=512,
                existing_data_behavior="delete_matching",
            )
            # for date_val, date_group in clean_data.groupby("date"):
            #     out_dir = os.path.join(output_base_dir, date_val)
            #     date_group.drop(columns=["date"]).to_parquet(
            #         out_dir,
            #         engine="pyarrow",
            #         partition_cols=["category"],
            #         index=False,
            #         max_partitions=2048,
            #         max_open_files=512,
            #         existing_data_behavior="delete_matching",
            #     )
            total_rows_written += len(clean_data)

    # Summary Report
    elapsed = time.time() - start_time
    print(f"--- Ingestion Summary ---")
    print(f"Total rows read: {total_rows_read}")
    print(f"Total rows written: {total_rows_written}")
    print(f"Number of bad records: {bad_records_count}")
    print(f"Execution time: {elapsed:.2f}s")

    # Warning if bad records exceed 5%
    if (
        total_rows_read > 0
        and (bad_records_count / total_rows_read) > BAD_RECORD_THRESHOLD
    ):
        print("WARNING: High data error rate! Bad records exceed 5% of total input.")


if __name__ == "__main__":
    validate_and_partition("datalake/raw/events.csv", "datalake/events/")
