from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
@dp.expect("valid_first_name", "first_name IS NOT NULL")
def customer_information():
    return( spark.read.table("samples.bakehouse.sales_customers")
           )