from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table
def zip_code_sales():
   bst = spark.read.table("bakehouse_sales_transactions")
   cust = spark.read.table("customer_information")

   temp = bst.join(cust, on="customerID", how="inner")

   temp2 = temp.groupBy("postal_zip_code").agg(F.sum("totalPrice").alias("total_sales"))

   return temp2