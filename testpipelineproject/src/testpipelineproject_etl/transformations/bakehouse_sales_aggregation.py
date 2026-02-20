from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

@dp.table
def bakehouse_sales_aggregation():
    return(
        spark.read.table("bakehouse_sales_transactions")
        .groupBy(col("franchiseID"))
        .agg(sum("totalPrice").alias("franTotal"))
    )