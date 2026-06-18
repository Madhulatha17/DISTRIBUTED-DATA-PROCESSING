from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder.appName("DistributedDataProcessing").getOrCreate()

df = spark.read.csv("data/sales_data.csv", header=True, inferSchema=True)

print("Original Data:")
df.show()

electronics_df = df.filter(col("category") == "Electronics")

print("Electronics Products:")
electronics_df.show()

category_sales = df.groupBy("category").agg(
    sum("quantity").alias("total_quantity"),
    avg("price").alias("avg_price")
)

print(" Category-wise Sales Summary:")
category_sales.show()

product_sales = df.groupBy("product").agg(
    sum("quantity").alias("total_quantity_sold")
)

print("Product-wise Sales:")
product_sales.show()

print("Total Records:", df.count())

spark.stop()

print(" Spark Job Completed Successfully")
