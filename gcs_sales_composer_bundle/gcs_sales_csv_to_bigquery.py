"""
Daily GCS CSV -> per-file BigQuery tables -> consolidated transformed table -> GCS archive.

Place config.yaml beside this DAG in the Composer dags/ folder.
The DAG uses Application Default Credentials from the Composer environment.
"""

import csv
import logging
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pendulum
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage


DAG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = DAG_DIR / "config.yaml"

with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
    CONFIG = yaml.safe_load(config_file)

PROJECT_ID = CONFIG["gcp"]["project_id"]
DATASET_ID = CONFIG["gcp"]["dataset_id"]
BQ_LOCATION = CONFIG["gcp"].get("location", "US")
BUCKET_NAME = CONFIG["gcs"]["bucket"]
SOURCE_PREFIX = CONFIG["gcs"]["source_prefix"].strip("/") + "/"
ARCHIVE_PREFIX = CONFIG["gcs"]["archive_prefix"].strip("/") + "/"
TIMEZONE = CONFIG["schedule"].get("timezone", "Asia/Kolkata")
TARGET_TABLE = CONFIG["bigquery"]["target_table"]
SOURCE_TABLE_PREFIX = CONFIG["bigquery"].get("source_table_prefix", "")
SOURCE_SCHEMA = CONFIG["source_schema"]

EXPECTED_COLUMNS = [field["name"] for field in SOURCE_SCHEMA]
BQ_SCHEMA = [
    bigquery.SchemaField(field["name"], field["type"], mode=field.get("mode", "NULLABLE"))
    for field in SOURCE_SCHEMA
]

DATASET_FQN = f"{PROJECT_ID}.{DATASET_ID}"
TARGET_FQN = f"{DATASET_FQN}.{TARGET_TABLE}"


def safe_table_name(relative_file_path: str) -> str:
    """Create a valid, deterministic BigQuery table name from a GCS object path."""
    stem = Path(relative_file_path).with_suffix("").as_posix().lower()
    table_name = re.sub(r"[^a-z0-9_]", "_", stem.replace("/", "_"))
    table_name = re.sub(r"_+", "_", table_name).strip("_")
    if not table_name:
        raise ValueError(f"Could not derive a BigQuery table name from: {relative_file_path}")
    if table_name[0].isdigit():
        table_name = f"file_{table_name}"
    return f"{SOURCE_TABLE_PREFIX}{table_name}"


def validate_csv_header(bucket, object_name: str) -> None:
    """Read only the header and fail fast if the file schema/order is unexpected."""
    blob = bucket.blob(object_name)
    with blob.open("r", encoding="utf-8-sig", newline="") as file_handle:
        header = next(csv.reader(file_handle), None)

    if header is None:
        raise ValueError(f"CSV is empty: gs://{BUCKET_NAME}/{object_name}")

    actual_columns = [column.strip() for column in header]
    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(
            f"Unexpected CSV header in gs://{BUCKET_NAME}/{object_name}. "
            f"Expected {EXPECTED_COLUMNS}; received {actual_columns}"
        )


def ensure_target_table(bq_client) -> None:
    """Create the dataset (if needed) and consolidated table."""
    dataset = bigquery.Dataset(DATASET_FQN)
    dataset.location = BQ_LOCATION
    bq_client.create_dataset(dataset, exists_ok=True)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_FQN}` (
      `Transaction_ID` STRING,
      `Date` DATE,
      `Time` TIME,
      `Store_ID` STRING,
      `Store_Location` STRING,
      `Product_ID` STRING,
      `Product_Name` STRING,
      `Category` STRING,
      `Quantity` INT64,
      `Unit_Price` NUMERIC,
      `Discount_Percent` NUMERIC,
      `Total_Amount` NUMERIC,
      `Payment_Method` STRING,
      `Customer_Type` STRING,
      `Cashier_ID` STRING,
      transaction_datetime DATETIME,
      gross_amount NUMERIC,
      discount_amount NUMERIC,
      net_amount NUMERIC,
      source_file STRING,
      processed_at TIMESTAMP
    )
    """
    bq_client.query(ddl, location=BQ_LOCATION).result()


def load_csv_to_file_table(bq_client, gcs_uri: str, table_fqn: str) -> None:
    """Load one CSV into its own BigQuery table. Re-runs replace that file table."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=BQ_SCHEMA,
        skip_leading_rows=1,
        field_delimiter=",",
        quote_character='"',
        encoding="UTF-8",
        allow_quoted_newlines=True,
        max_bad_records=0,
        autodetect=False,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_fqn,
        job_config=job_config,
        location=BQ_LOCATION,
    )
    load_job.result()
    logging.info("Loaded %s rows from %s into %s", load_job.output_rows, gcs_uri, table_fqn)


def merge_into_target(bq_client, source_table_fqn: str, source_file: str) -> None:
    """
    Upsert into the consolidated table using (source_file, Transaction_ID).
    This makes retries safe if a task fails after the MERGE but before archiving.
    """
    source_file_sql = source_file.replace("'", "''")
    sql = f"""
    MERGE `{TARGET_FQN}` AS T
    USING (
      SELECT
        NULLIF(TRIM(`Transaction_ID`), '') AS `Transaction_ID`,
        `Date`,
        `Time`,
        NULLIF(TRIM(`Store_ID`), '') AS `Store_ID`,
        NULLIF(TRIM(`Store_Location`), '') AS `Store_Location`,
        NULLIF(TRIM(`Product_ID`), '') AS `Product_ID`,
        NULLIF(TRIM(`Product_Name`), '') AS `Product_Name`,
        NULLIF(TRIM(`Category`), '') AS `Category`,
        `Quantity`,
        `Unit_Price`,
        `Discount_Percent`,
        `Total_Amount`,
        NULLIF(TRIM(`Payment_Method`), '') AS `Payment_Method`,
        NULLIF(TRIM(`Customer_Type`), '') AS `Customer_Type`,
        NULLIF(TRIM(`Cashier_ID`), '') AS `Cashier_ID`,
        DATETIME(`Date`, `Time`) AS transaction_datetime,
        CAST(`Quantity` AS NUMERIC) * `Unit_Price` AS gross_amount,
        (CAST(`Quantity` AS NUMERIC) * `Unit_Price`)
          * `Discount_Percent` / 100 AS discount_amount,
        `Total_Amount` AS net_amount,
        '{source_file_sql}' AS source_file,
        CURRENT_TIMESTAMP() AS processed_at
      FROM `{source_table_fqn}`
      WHERE NULLIF(TRIM(`Transaction_ID`), '') IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(`Transaction_ID`)
        ORDER BY `Date` DESC, `Time` DESC
      ) = 1
    ) AS S
    ON T.source_file = S.source_file
       AND T.Transaction_ID = S.Transaction_ID
    WHEN MATCHED THEN UPDATE SET
      T.Date = S.Date,
      T.Time = S.Time,
      T.Store_ID = S.Store_ID,
      T.Store_Location = S.Store_Location,
      T.Product_ID = S.Product_ID,
      T.Product_Name = S.Product_Name,
      T.Category = S.Category,
      T.Quantity = S.Quantity,
      T.Unit_Price = S.Unit_Price,
      T.Discount_Percent = S.Discount_Percent,
      T.Total_Amount = S.Total_Amount,
      T.Payment_Method = S.Payment_Method,
      T.Customer_Type = S.Customer_Type,
      T.Cashier_ID = S.Cashier_ID,
      T.transaction_datetime = S.transaction_datetime,
      T.gross_amount = S.gross_amount,
      T.discount_amount = S.discount_amount,
      T.net_amount = S.net_amount,
      T.processed_at = S.processed_at
    WHEN NOT MATCHED THEN INSERT (
      `Transaction_ID`, `Date`, `Time`, `Store_ID`, `Store_Location`,
      `Product_ID`, `Product_Name`, `Category`, `Quantity`, `Unit_Price`,
      `Discount_Percent`, `Total_Amount`, `Payment_Method`, `Customer_Type`,
      `Cashier_ID`, transaction_datetime, gross_amount, discount_amount,
      net_amount, source_file, processed_at
    ) VALUES (
      S.`Transaction_ID`, S.`Date`, S.`Time`, S.`Store_ID`, S.`Store_Location`,
      S.`Product_ID`, S.`Product_Name`, S.`Category`, S.`Quantity`, S.`Unit_Price`,
      S.`Discount_Percent`, S.`Total_Amount`, S.`Payment_Method`, S.`Customer_Type`,
      S.`Cashier_ID`, S.transaction_datetime, S.gross_amount, S.discount_amount,
      S.net_amount, S.source_file, S.processed_at
    )
    """
    bq_client.query(sql, location=BQ_LOCATION).result()


def archive_gcs_object(bucket, source_object: str, relative_path: str) -> None:
    """Copy to a dated archive path, then delete the source only after copy completes."""
    run_date = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y/%m/%d")
    destination_object = f"{ARCHIVE_PREFIX}{run_date}/{relative_path}"
    source_blob = bucket.blob(source_object)
    destination_blob = bucket.blob(destination_object)

    rewrite_token = None
    while True:
        rewrite_token, _, _ = destination_blob.rewrite(source_blob, token=rewrite_token)
        if rewrite_token is None:
            break

    # Delete only after the destination copy has completed.
    source_blob.delete()
    logging.info(
        "Archived gs://%s/%s to gs://%s/%s",
        BUCKET_NAME, source_object, BUCKET_NAME, destination_object
    )


def process_source_files(**context) -> None:
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    ensure_target_table(bq_client)

    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=SOURCE_PREFIX))
    csv_blobs = [
        blob for blob in blobs
        if not blob.name.endswith("/")
        and blob.name.lower().endswith(".csv")
    ]

    if not csv_blobs:
        logging.info("No CSV files found under gs://%s/%s", BUCKET_NAME, SOURCE_PREFIX)
        return

    # Stable ordering makes runs easier to audit.
    csv_blobs.sort(key=lambda blob: blob.name)

    for blob in csv_blobs:
        object_name = blob.name
        relative_path = object_name[len(SOURCE_PREFIX):]
        table_name = safe_table_name(relative_path)
        source_table_fqn = f"{DATASET_FQN}.{table_name}"
        gcs_uri = f"gs://{BUCKET_NAME}/{object_name}"

        logging.info("Starting processing for %s", gcs_uri)

        # If any step below fails, the object remains in source-files for retry.
        validate_csv_header(bucket, object_name)
        load_csv_to_file_table(bq_client, gcs_uri, source_table_fqn)
        merge_into_target(bq_client, source_table_fqn, object_name)
        archive_gcs_object(bucket, object_name, relative_path)

        logging.info("Successfully processed and archived %s", gcs_uri)


local_tz = pendulum.timezone(TIMEZONE)

with DAG(
    dag_id="gcs_sales_csv_to_bigquery",
    description="Load each GCS sales CSV to its own BQ table, merge into target, and archive",
    schedule=CONFIG["schedule"]["cron"],
    start_date=pendulum.datetime(2026, 11, 17, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["gcs", "bigquery", "sales", "event-based-trigger"],
) as dag:

    process_files = PythonOperator(
        task_id="process_csv_files",
        python_callable=process_source_files,
    )
