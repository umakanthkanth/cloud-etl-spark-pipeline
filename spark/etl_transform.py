from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import gspread
import os
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/content/beaming-park-464705-d9-12b873d9a2a9.json", scope)
client = gspread.authorize(creds)
def create_spark_session():
    spark = SparkSession.builder \
        .appName("StoreSalesETL") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()
    return spark
def load_sales_data(spark, file_path):
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", '"') \
        .csv(file_path)
    df.printSchema()
    df.show(5, truncate=False)
    return df
def transform_sales_data(df):
    # Clean column names
    cleaned_columns = []
    for col_name in df.columns:
        cleaned = col_name.strip().replace(" ", "_").replace('"', '')
        df = df.withColumnRenamed(col_name, cleaned)
        cleaned_columns.append(cleaned)
    print("Cleaned Columns:", cleaned_columns)
    required_columns = ["Month", "1958", "1959", "1960"]
    df = df.dropna(subset=required_columns)
    df = df.withColumn("Total_1958_to_1960", col("1958") + col("1959") + col("1960"))
    return df
def run_data_quality_checks(df):
    dq_results = []
    rule_id = "DQ001"
    rule_description = "Total_1958_to_1960 should be greater than 0"
    failed_count = df.filter(col("Total_1958_to_1960") <= 0).count()
    if failed_count > 0:
        dq_results.append((rule_id, rule_description, failed_count))
    return dq_results
if __name__ == "__main__":
    spark = create_spark_session()
    file_path = "/content/airtravel.csv"
    sales_df = load_sales_data(spark, file_path)
    print("Before transformation (row count):", sales_df.count())
    transformed_df = transform_sales_data(sales_df)
    print("After transformation (row count):", transformed_df.count())
    transformed_df.printSchema()
    dq_results = run_data_quality_checks(transformed_df)
    if dq_results:
        print("❌ Data Quality Check Failed!")
        for rule in dq_results:
            print(f"Rule ID: {rule[0]}")
            print(f"Description: {rule[1]}")
            print(f"Failed Rows: {rule[2]}")
    else:
        print("✅ All Data Quality Checks Passed!")
        # ✅ Step 8: Create or open sheet safely
        sheet_title = "store_sales_output"
        try:
            spreadsheet = client.open(sheet_title)
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = client.create(sheet_title)
        sheet = spreadsheet.get_worksheet(0)
        pandas_df = transformed_df.toPandas()
        set_with_dataframe(sheet, pandas_df)
        print("Data successfully written to Google Sheet!")
        spreadsheet.share('anilent8@gmail.com', perm_type='user', role='writer')
        print("Shared Google Sheet with your Gmail account!")
        print("Sheet URL:", spreadsheet.url)
