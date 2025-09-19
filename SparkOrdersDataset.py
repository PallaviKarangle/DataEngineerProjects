
from pyspark.sql import SparkSession
import getpass

# Get local username
username = getpass.getuser()

# Create Spark session in local mode
spark = (SparkSession.builder
    .appName("LocalSparkOrders")
    .master("local[*]")   # run locally using all CPU cores
    .config("spark.ui.port", "0")
    .config("spark.shuffle.useOldFetchProtocol", "true")
    .config("spark.sql.warehouse.dir", f"/home/pallavi_karangle/warehouse")  # Linux/Mac
    # For Windows, use instead:
    # .config("spark.sql.warehouse.dir", f"C:/Users/pallavi_karangle/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# -------------------------
# Load local dataset
# -------------------------

orders_rdd = spark.sparkContext.textFile("data/orders.csv")

# Preview
print(orders_rdd.take(10))

# -------------------------
# RDD transformations
# -------------------------

# Count orders under each status
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
reduced_sorted = reduced_rdd.sortBy(lambda x: x[1], False)
print(reduced_sorted.collect())

# Premium customers (top 10 customers who placed most orders)
customers_mapped = orders_rdd.map(lambda x: (x.split(",")[2], 1))
customers_aggregated = customers_mapped.reduceByKey(lambda x, y: x + y)
customers_sorted = customers_aggregated.sortBy(lambda x: x[1], False)
print(customers_sorted.take(10))

# Distinct customers
distinct_customers = orders_rdd.map(lambda x: x.split(",")[2]).distinct()
print("Unique customers:", distinct_customers.count())

# Total number of orders
print("Total orders:", orders_rdd.count())

# Orders that are CLOSED
filtered_orders = orders_rdd.filter(lambda x: (x.split(",")[3] == "CLOSED"))
filtered_mapped = filtered_orders.map(lambda x: (x.split(",")[2], 1))
filtered_aggregated = filtered_mapped.reduceByKey(lambda x, y: x + y)
filtered_sorted = filtered_aggregated.sortBy(lambda x: x[1], False)
print(filtered_sorted.take(10))



