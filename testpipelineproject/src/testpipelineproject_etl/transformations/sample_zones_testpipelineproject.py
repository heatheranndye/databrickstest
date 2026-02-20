from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_zones_testpipelineproject():
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.read.table(f"sample_trips_testpipelineproject")
        .groupBy(col("city"))
        .agg(sum("size").alias("total_size"))
    )
