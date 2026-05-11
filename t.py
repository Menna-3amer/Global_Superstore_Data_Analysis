import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

os.environ["HADOOP_USER_NAME"] = "root"

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('SalesStarSchemaTransformation') \
    .master('local[*]') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.yarn.am.memory", "1g") \
    .getOrCreate()

print("Spark Connected Successfully")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ── Paths ──────────────────────────────────────────────────────────────────────
HDFS_BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sales_data/"
GOLD_BASE_PATH   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/sales/"

# ── Helper: Date Dimension ─────────────────────────────────────────────────────
def generate_static_date_dim(spark, start_date="2011-01-01", end_date="2015-12-31"):
    df = spark.sql(f"SELECT CAST('{start_date}' AS DATE) as start, CAST('{end_date}' AS DATE) as end")
    df = df.select(
        F.explode(
            F.sequence(F.to_date("start"), F.to_date("end"), F.expr("interval 1 day"))
        ).alias("date")
    )
    dim_date = df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("week_of_year"),
        F.quarter("date").alias("quarter"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
    return dim_date

# ── Load Bronze Data ───────────────────────────────────────────────────────────
df_silver = spark.read.parquet(HDFS_BRONZE_PATH)
print(f"Loaded {df_silver.count()} records from Bronze layer")

df_silver = df_silver \
    .withColumn("Order_Date",    F.to_date("Order_Date", "yyyy-MM-dd")) \
    .withColumn("arrival_time",  F.to_timestamp("arrival_time"))

# ── dim_date ───────────────────────────────────────────────────────────────────
try:
    spark.read.parquet(f"{GOLD_BASE_PATH}dim_date")
    print("dim_date already exists. Skipping generation.")
except Exception:
    print("Generating dim_date...")
    dim_date = generate_static_date_dim(spark)
    dim_date.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_date")
    print("dim_date created")

# ── dim_customer ───────────────────────────────────────────────────────────────
dim_customer = df_silver.select("Customer_ID", "Customer_Name", "Segment").distinct() \
    .withColumn("customer_type", F.lit("Retail")) \
    .withColumn("created_date",  F.current_date())

try:
    existing = spark.read.parquet(f"{GOLD_BASE_PATH}dim_customer")
    dim_customer = existing.unionByName(dim_customer).dropDuplicates(["Customer_ID"])
    print("Merged Customer Dimension")
except:
    print("Creating new Customer Dimension")

dim_customer.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_customer")

# ── dim_product ────────────────────────────────────────────────────────────────
dim_product = df_silver.select("Product_ID", "Product_Name", "Category", "Sub_Category").distinct() \
    .withColumn("product_category_code", F.regexp_replace(F.col("Category"), " ", "_"))

try:
    existing = spark.read.parquet(f"{GOLD_BASE_PATH}dim_product")
    dim_product = existing.unionByName(dim_product).dropDuplicates(["Product_ID"])
    print("Merged Product Dimension")
except:
    print("Creating new Product Dimension")

dim_product.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_product")

# ── dim_location ───────────────────────────────────────────────────────────────
dim_location = df_silver.select("City", "State", "Country", "Region").distinct() \
    .withColumn("location_key", F.monotonically_increasing_id())

try:
    existing = spark.read.parquet(f"{GOLD_BASE_PATH}dim_location")
    dim_location = existing.unionByName(dim_location).dropDuplicates(["City", "State", "Country"])
    print("Merged Location Dimension")
except:
    print("Creating new Location Dimension")

dim_location.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_location")

# ── dim_order ──────────────────────────────────────────────────────────────────
dim_order = df_silver.select("Order_ID", "Order_Priority", "Ship_Mode").distinct() \
    .withColumn("order_type", F.lit("Standard"))

try:
    existing = spark.read.parquet(f"{GOLD_BASE_PATH}dim_order")
    dim_order = existing.unionByName(dim_order).dropDuplicates(["Order_ID"])
    print("Merged Order Dimension")
except:
    print("Creating new Order Dimension")

dim_order.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_order")

# ── fact_sales ─────────────────────────────────────────────────────────────────
fact_sales = df_silver.withColumn(
    "date_key", F.date_format("Order_Date", "yyyyMMdd").cast("int")
).select(
    "Order_ID",
    "Customer_ID",
    "Product_ID",
    "City",
    "State",
    "Country",
    "date_key",
    F.col("Sales").cast(DoubleType()).alias("sales_amount"),
    F.col("Quantity").cast(IntegerType()).alias("quantity"),
    F.col("Profit").cast(DoubleType()).alias("profit_amount"),
    F.col("Discount").cast(DoubleType()).alias("discount_rate"),
    F.col("Shipping_Cost").cast(DoubleType()).alias("shipping_cost"),
    "batch_id",
    "arrival_time"
)

fact_sales.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_sales")

# ── Summary ────────────────────────────────────────────────────────────────────
print("Gold Layer Created Successfully")
print("dim_date:",     f"{GOLD_BASE_PATH}dim_date")
print("dim_customer:", f"{GOLD_BASE_PATH}dim_customer")
print("dim_product:",  f"{GOLD_BASE_PATH}dim_product")
print("dim_location:", f"{GOLD_BASE_PATH}dim_location")
print("dim_order:",    f"{GOLD_BASE_PATH}dim_order")
print("fact_sales:",   f"{GOLD_BASE_PATH}fact_sales")

print("\nSample fact_sales:")
fact_sales.show(5)

print("Sample dim_customer:")
dim_customer.show(5)

print("Sample dim_product:")
dim_product.show(5)

spark.stop()
print("Spark Session Stopped")

