import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, to_date, to_timestamp

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('SalesDataETL') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

print("Spark Connected Successfully")

# Schema for sales data (matching your CSV columns)
sales_schema = StructType([
    StructField("Order.ID", StringType(), True),
    StructField("Order.Date", StringType(), True),
    StructField("Customer.ID", StringType(), True),
    StructField("Customer.Name", StringType(), True),
    StructField("Product.ID", StringType(), True),
    StructField("Product.Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Shipping.Cost", DoubleType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub.Category", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Order.Priority", StringType(), True),
    StructField("Ship.Mode", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Market", StringType(), True),
    StructField("batch_id", IntegerType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("simulated_customer_group", StringType(), True),
    StructField("event_time", StringType(), True)
])

# Path to your CSV batches (local path for now, can be changed to HDFS)
input_path = "file:/home/jovyan/work/data_batches"

print(f"Reading batch data from: {input_path}")

try:
    # Read all CSV files from the batches folder
    raw_df = spark.read \
        .schema(sales_schema) \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .csv(input_path)
    
    raw_df = raw_df.toDF(*[c.replace(".", "_").strip() for c in raw_df.columns])
    
    record_count = raw_df.count()
    print(f"Total records read: {record_count}")

    if record_count > 0:
        # Clean data: convert date strings to proper date format
        clean_df = raw_df \
            .withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("arrival_time", to_timestamp(col("arrival_time"), "yyyy-MM-dd HH:mm:ss"))
        
        hdfs_output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sales_data/"
        
        print(f"Processing {record_count} records...")
        print(f"Writing to HDFS (Bronze Layer): {hdfs_output_path}")
        
        # Save as Parquet
        clean_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(hdfs_output_path)
        spark.read.parquet(hdfs_output_path).printSchema()
        print("Batch ingestion complete. Data saved as Parquet in HDFS.")
        
        # Show sample
        print("Sample data:")
        clean_df.show(5)
    else:
        print("No new data found in the batches folder.")

except Exception as e:
    print(f"Error during Spark processing: {e}")

finally:
    spark.stop()
    print("Spark Session Stopped.")