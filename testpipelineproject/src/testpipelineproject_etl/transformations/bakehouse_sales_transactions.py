from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table
def bakehouse_sales_transactions():
    return( spark.read.table("samples.bakehouse.sales_transactions")
    .select("transactionID", "franchiseID", "customerID", "product", "totalPrice" ))

    