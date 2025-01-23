import zipfile
from datetime import datetime

import requests
from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, date_sub, lit, max, sum
from pyspark.sql.utils import AnalysisException

# Constants for paths and URLs
BASE_PATH = "/mnt/my_storage"
URL = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/csv/data.csv"
TIMEOUT = 60

TABLE_NAME = "covid"
CSV_FILE_PATH = f"{BASE_PATH}/{TABLE_NAME}.csv"
ZIP_FILE_PATH = f"{BASE_PATH}/{TABLE_NAME}.zip"

LATEST_FIVE_DAYS_TABLE = "covid_latest_five_days"
TOTAL_CASES_TABLE = "covid_total_cases"

LATEST_FIVE_DAYS_CSV = f"/dbfs/tmp/{LATEST_FIVE_DAYS_TABLE}.csv"
TOTAL_CASES_CSV = f"/dbfs/tmp/{TOTAL_CASES_TABLE}.csv"

LATEST_FIVE_DAYS_TABLE_PATH = f"{BASE_PATH}/{LATEST_FIVE_DAYS_TABLE}"
TOTAL_CASES_TABLE_PATH = f"{BASE_PATH}/{TOTAL_CASES_TABLE}"


def fetch_data_availability() -> requests.Response | None:
    """Check if the data source is available."""
    try:
        response = requests.head(URL, timeout=TIMEOUT)
        if response.status_code == 200:
            return response
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
    except requests.exceptions.RequestException as exc:
        print(f"Error checking data source: {exc}")
    return None


def get_last_modified_date_from_headers(response: requests.Response) -> datetime:
    """Get the last modified date of the source data from headers."""
    format_ = "%a, %d %b %Y %H:%M:%S %Z"
    return datetime.strptime(
        response.headers.get("Last-Modified", datetime.now().strftime(format_)), format_
    )


def get_last_modified_date_from_delta() -> datetime:
    """Get the last modified date from the Delta table's history."""
    try:
        history_df = DeltaTable.forName(spark, TABLE_NAME).history()
        return datetime.strptime(
            history_df.orderBy(history_df["timestamp"].desc()).first()["timestamp"],
            "%Y-%m-%d %H:%M:%S",
        )
    except AnalysisException:
        return datetime.min


def download_and_create_delta_table() -> DataFrame:
    """Download the CSV file and create a Delta table."""
    dbutils.fs.cp(URL, CSV_FILE_PATH)
    df = spark.read.csv(CSV_FILE_PATH, header=True, inferSchema=True)
    df.write.mode("overwrite").saveAsTable(TABLE_NAME)
    return df


def process_last_five_days(df: DataFrame) -> None:
    """Process data for the last 5 days."""
    max_date = df.agg(max("dateRep")).collect()[0][0]
    df_transformed = df.filter(col("dateRep").between(date_sub(lit(max_date), 5), max_date))
    df_transformed.write.mode("overwrite").saveAsTable(LATEST_FIVE_DAYS_TABLE_PATH)
    df_transformed.coalesce(1).write.option("header", "true").csv(LATEST_FIVE_DAYS_CSV)


def process_total_cases(df: DataFrame) -> None:
    """Process total cases data by country."""
    df_transformed = df.groupBy("countriesAndTerritories").agg(sum("cases").alias("total_cases"))
    df_transformed.write.mode("overwrite").saveAsTable(TOTAL_CASES_TABLE_PATH)
    df_transformed.coalesce(1).write.option("header", "true").csv(TOTAL_CASES_CSV)


def save_to_zip() -> None:
    """Save processed files into a zip archive."""
    with zipfile.ZipFile(ZIP_FILE_PATH, "w") as zipf:
        zipf.write(LATEST_FIVE_DAYS_CSV)
        zipf.write(TOTAL_CASES_CSV)


def main() -> None:
    """Main orchestration function."""
    response = fetch_data_availability()
    if not response:
        return

    if get_last_modified_date_from_delta() >= get_last_modified_date_from_headers(response):
        return

    df = download_and_create_delta_table()
    process_last_five_days(df)
    process_total_cases(df)
    save_to_zip()


if __name__ == "__main__":
    main()
