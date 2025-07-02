from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os
load_dotenv()
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL")
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL")
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

# Step 2: Load data
def load_sales_data(spark, file_path):
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", '"') \
        .csv(file_path)
        
    df.printSchema()
    df.show(5, truncate=False)
    return df


# Step 3: Transform data
def transform_sales_data(df):
    # Clean column names: strip spaces and quotes
    cleaned_columns = []
    for col_name in df.columns:
        cleaned = col_name.strip().replace(" ", "_").replace('"', '')
        df = df.withColumnRenamed(col_name, cleaned)
        cleaned_columns.append(cleaned)

    # Confirm names are cleaned
    print("Cleaned Columns:", cleaned_columns)

    # Define required columns for DQ
    required_columns = ["Month", "1958", "1959", "1960"]
    df = df.dropna(subset=required_columns)

    # Add derived column
    df = df.withColumn("Total_1958_to_1960", col("1958") + col("1959") + col("1960"))

    return df


# Step 4: Run DQ checks
def run_data_quality_checks(df):
    dq_results = []
    rule_id = "DQ001"
    rule_description = "Total_1958_to_1960 should be greater than 0"
    failed_count = df.filter(col("Total_1958_to_1960") <= 0).count()
    if failed_count > 0:
        dq_results.append((rule_id, rule_description, failed_count))
    return dq_results
    
def send_dq_alert(failed_rules):
    subject = "❌ Data Quality Alert - DQ Check Failed"
    content_lines = ["One or more DQ checks failed in your ETL pipeline:\n"]
    
    for rule_id, description, count in failed_rules:
        content_lines.append(f"🔹 Rule ID: {rule_id}")
        content_lines.append(f"   Description: {description}")
        content_lines.append(f"   Failed Rows: {count}\n")
    
    email_body = "\n".join(content_lines)

    message = Mail(
        from_email=ALERT_FROM_EMAIL,
        to_emails=ALERT_TO_EMAIL,
        subject=subject,
        plain_text_content=email_body
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"✅ Alert sent! Status Code: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send alert: {str(e)}")
# Main block
if __name__ == "__main__":
    # Step 1: Create SparkSession
    spark = create_spark_session()

    # Step 2: Define path to the CSV file (downloaded locally in Colab)
    file_path = "/content/airtravel.csv"

    # Step 3: Load raw data into a DataFrame
    sales_df = load_sales_data(spark, file_path)

    # Optional: Count before transformation
    print("Before transformation (row count):", sales_df.count())

    # Step 4: Apply transformation logic
    transformed_df = transform_sales_data(sales_df)

    # Optional: Count after transformation
    print("After transformation (row count):", transformed_df.count())

    # Optional: Show schema to verify cleaned columns
    transformed_df.printSchema()

    # Step 5: Run DQ check
    dq_results = run_data_quality_checks(transformed_df)

    if dq_results:
        print("❌ Data Quality Check Failed!")
        for rule in dq_results:
            print(f"Rule ID: {rule[0]}")
            print(f"Description: {rule[1]}")
            print(f"Failed Rows: {rule[2]}")
    else:
        print("All Data Quality Checks Passed")
    
    



