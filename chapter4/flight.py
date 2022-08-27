from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import StringType, StructField

spark = SparkSession.builder.appName("Example").getOrCreate()
schema = (
    "date STRING, delay INTEGER, distance INTEGER, origin STRING, destination STRING"
)
df = spark.read.csv(
    "../databricks-datasets/learning-spark-v2/flights/departuredelays.csv",
    schema=schema,
    header=True,
)
df.select(["delay", "origin", "destination"]).withColumn(
    "flight_delays", when(col("delay") > 360, "Very Long Delays").otherwise("else")
).orderBy(["origin", "delay"], ascending=[True, False]).show(5, False)

# df.createOrReplaceTempView("us_delay_flights_tbl")
# spark.sql(
#     """SELECT delay, origin, destination,
#         CASE
#             WHEN delay > 360 THEN 'Very Long Delays'
#             WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
#             WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
#             WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
#             WHEN delay = 0 THEN 'No Delays'
#             ELSE 'Early'
#         END AS flight_delays
#         FROM us_delay_flights_tbl
#         ORDER BY origin, delay DESC
#     """
# ).show(10)
