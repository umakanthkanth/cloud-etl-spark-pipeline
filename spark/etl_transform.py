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
if __name__ == "__main__":
    spark = create_spark_session()
    file_path = "/content/store_sales.csv"  # Replace with GCS path later
    sales_df = load_sales_data(spark, file_path)
    sales_df.show(5)

