from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
        .csv(file_path)
    df.printSchema()
    df.show(5, truncate=False)
    return df
def transform_sales_data(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip().replace(" ", "_"))
    required_columns = ["Month", "1958", "1959", "1960"]  # Adjust if different in your data
    df = df.dropna(subset=required_columns)
    df = df.withColumn("Total_1958_to_1960", col("1958") + col("1959") + col("1960"))
    return df
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

    # Optional: Preview first few rows (uncomment if needed)
    # transformed_df.show(5)



