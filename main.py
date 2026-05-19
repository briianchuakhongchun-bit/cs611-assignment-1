import glob
import os
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession

import utils.data_processing_bronze_table as bronze
import utils.data_processing_silver_table as silver
import utils.data_processing_gold_table as gold


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def generate_first_of_month_dates(start_date_str: str, end_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    out, current = [], datetime(start_date.year, start_date.month, 1)
    while current <= end_date:
        out.append(current.strftime("%Y-%m-%d"))
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    return out


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


# ----------------------------------------------------------------------------
# Spark
# ----------------------------------------------------------------------------
spark = (SparkSession.builder.appName("assignment1").master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# ----------------------------------------------------------------------------
# Conditions Configuration
# ----------------------------------------------------------------------------
START_DATE = "2023-01-01"
END_DATE = "2024-12-01"
DPD_THRESHOLD = 30
MOB_THRESHOLD = 6

dates_str_lst = generate_first_of_month_dates(START_DATE, END_DATE)
print(f"Processing {len(dates_str_lst)} monthly partitions from {START_DATE} to {END_DATE}")

# Datamart directory layout
DM = "datamart"
BRONZE_DIRS = {
    "lms":         os.path.join(DM, "bronze", "lms"),
    "clickstream": os.path.join(DM, "bronze", "clickstream"),
    "attributes":  os.path.join(DM, "bronze", "attributes"),
    "financials":  os.path.join(DM, "bronze", "financials"),
}
SILVER_DIRS = {
    "lms":         os.path.join(DM, "silver", "lms"),
    "clickstream": os.path.join(DM, "silver", "clickstream"),
    "attributes":  os.path.join(DM, "silver", "attributes"),
    "financials":  os.path.join(DM, "silver", "financials"),
}
GOLD_LABEL_DIR   = os.path.join(DM, "gold", "label_store")
GOLD_FEATURE_DIR = os.path.join(DM, "gold", "feature_store")

for d in list(BRONZE_DIRS.values()) + list(SILVER_DIRS.values()) + [GOLD_LABEL_DIR, GOLD_FEATURE_DIR]: ensure_dir(d)


# ----------------------------------------------------------------------------
# Bronze backfill
# ----------------------------------------------------------------------------
print("\n========== BRONZE ==========")
for date_str in dates_str_lst:
    bronze.process_bronze_lms(date_str, BRONZE_DIRS["lms"], spark)
    bronze.process_bronze_clickstream(date_str, BRONZE_DIRS["clickstream"], spark)
    bronze.process_bronze_attributes(date_str, BRONZE_DIRS["attributes"], spark)
    bronze.process_bronze_financials(date_str, BRONZE_DIRS["financials"], spark)


# ----------------------------------------------------------------------------
# Silver backfill
# ----------------------------------------------------------------------------
print("\n========== SILVER ==========")
for date_str in dates_str_lst:
    silver.process_silver_lms(date_str, BRONZE_DIRS["lms"], SILVER_DIRS["lms"], spark)
    silver.process_silver_clickstream(date_str, BRONZE_DIRS["clickstream"], SILVER_DIRS["clickstream"], spark)
    silver.process_silver_attributes(date_str, BRONZE_DIRS["attributes"], SILVER_DIRS["attributes"], spark)
    silver.process_silver_financials(date_str, BRONZE_DIRS["financials"], SILVER_DIRS["financials"], spark)


# ----------------------------------------------------------------------------
# Gold backfill
# ----------------------------------------------------------------------------
print("\n========== GOLD ==========")
for date_str in dates_str_lst:
    gold.process_gold_label_store(date_str, SILVER_DIRS["lms"], GOLD_LABEL_DIR, spark, dpd=DPD_THRESHOLD, mob=MOB_THRESHOLD)

for date_str in dates_str_lst:
    gold.process_gold_feature_store(date_str, SILVER_DIRS["clickstream"], SILVER_DIRS["attributes"], SILVER_DIRS["financials"], GOLD_FEATURE_DIR, spark)


# ----------------------------------------------------------------------------
# Double check data
# ----------------------------------------------------------------------------
print("\n========== Data Validation ==========")
label_files = glob.glob(os.path.join(GOLD_LABEL_DIR, "*.parquet"))
feature_files = glob.glob(os.path.join(GOLD_FEATURE_DIR, "*.parquet"))
print(f"gold/label_store partitions:   {len(label_files)}")
print(f"gold/feature_store partitions: {len(feature_files)}")

if label_files:
    df_labels = spark.read.parquet(*label_files)
    print(f"label store total rows: {df_labels.count()}")
    df_labels.show(5)

if feature_files:
    df_features = spark.read.parquet(*feature_files)
    print(f"feature store total rows: {df_features.count()}; cols: {len(df_features.columns)}")
    df_features.show(5)

print("\nData Processing Complete.")
