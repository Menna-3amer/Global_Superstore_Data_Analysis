# ============================================
# Cell 1: Imports
# ============================================
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

print("Imports Done")

# ============================================
# Cell 2: Create Spark Session
# ============================================
spark = SparkSession.builder \
    .appName('LoadToSnowflake') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.maxResultSize", "512m") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config(
        "spark.jars.packages",
        "net.snowflake:snowflake-jdbc:3.13.33,"
        "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"
    ) \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
print("Spark Session Created")

# ============================================
# Cell 3: Snowflake Connection Options
# ============================================
sfOptions = {
    "sfURL": "https://rx26256.eu-central-2.aws.snowflakecomputing.com",
    "sfUser": "menna18",
    "sfPassword": "Zn3ueYWfKvgmxxj",
    "sfDatabase": "SALES_STAR_DWH",
    "sfSchema": "STAR",
    "sfWarehouse": "ETL_WH",
    "sfRole": "ACCOUNTADMIN"
}

print("Snowflake options configured")

# ============================================
# Cell 4: Define Gold Path
# ============================================
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/sales/"

print(f" Reading from: {GOLD_BASE_PATH}")


# ============================================
# Cell 5: Read FACT_SALES
# ============================================
fact_sales_df = spark.read.parquet(f"{GOLD_BASE_PATH}fact_sales")

print(f" FACT_SALES rows: {fact_sales_df.count()}")
fact_sales_df.show(5)



# ============================================
# Cell 6: Read DIM_CUSTOMER
# ============================================
dim_customer_df = spark.read.parquet(f"{GOLD_BASE_PATH}dim_customer")

print(f" DIM_CUSTOMER rows: {dim_customer_df.count()}")
dim_customer_df.show(5)


# ============================================
# Cell 7: Read DIM_PRODUCT
# ============================================
dim_product_df = spark.read.parquet(f"{GOLD_BASE_PATH}dim_product")

print(f" DIM_PRODUCT rows: {dim_product_df.count()}")
dim_product_df.show(5)

# ============================================
# Cell 8: Read DIM_LOCATION
# ============================================
dim_location_df = spark.read.parquet(f"{GOLD_BASE_PATH}dim_location")

print(f" DIM_LOCATION rows: {dim_location_df.count()}")
dim_location_df.show(5)

# ============================================
# Cell 9: Read DIM_ORDER
# ============================================
dim_order_df = spark.read.parquet(f"{GOLD_BASE_PATH}dim_order")

print(f" DIM_ORDER rows: {dim_order_df.count()}")
dim_order_df.show(5)

# ============================================
# Cell 10: Read DIM_DATE
# ============================================
dim_date_df = spark.read.parquet(f"{GOLD_BASE_PATH}dim_date")

print(f" DIM_DATE rows: {dim_date_df.count()}")
dim_date_df.show(5)


# ============================================
# Cell 11: Load FACT_SALES to Snowflake
# ============================================
print(" Loading FACT_SALES...")

fact_sales_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "FACT_SALES") \
    .mode("append") \
    .save()

print(" FACT_SALES loaded")

# ============================================
# Cell 12: Load DIM_CUSTOMER to Snowflake
# ============================================
print("Loading DIM_CUSTOMER...")

dim_customer_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_CUSTOMER") \
    .mode("overwrite") \
    .save()

print("DIM_CUSTOMER loaded")


# ============================================
# Cell 13: Load DIM_PRODUCT to Snowflake
# ============================================
print("Loading DIM_PRODUCT...")

dim_product_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_PRODUCT") \
    .mode("overwrite") \
    .save()

print(" DIM_PRODUCT loaded")

# ============================================
# Cell 14: Load DIM_LOCATION to Snowflake
# ============================================
print("Loading DIM_LOCATION...")

dim_location_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_LOCATION") \
    .mode("overwrite") \
    .save()

print(" DIM_LOCATION loaded")


# ============================================
# Cell 15: Load DIM_ORDER to Snowflake
# ============================================
print("Loading DIM_ORDER...")

dim_order_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_ORDER") \
    .mode("overwrite") \
    .save()

print(" DIM_ORDER loaded")

# ============================================
# Cell 16: Load DIM_DATE to Snowflake
# ============================================
print("Loading DIM_DATE...")

dim_date_df.write.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_DATE") \
    .mode("overwrite") \
    .save()

print("DIM_DATE loaded")


# ============================================
# Cell 17: Finish Message
# ============================================
print("ALL DATA LOADED TO SNOWFLAKE SUCCESSFULLY!")

# ============================================
# Cell 18: Stop Spark Session
# ============================================
spark.stop()

print("Spark Session Stopped")





