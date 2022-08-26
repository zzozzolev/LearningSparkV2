import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year
from pyspark.sql.types import *

with open("./fire.avsc") as f:
    fire_avsc = json.load(f)

schema = StructType.fromJson(fire_avsc)
spark = SparkSession.builder.appName("fire").getOrCreate()
fire_df = spark.read.csv("../../data/sf-fire-calls.csv", header=True, schema=schema)

fire_ts_df = (
    fire_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)
fire_ts_df.select("CallType").where(col("CallType").isNotNull()).groupBy(
    "CallType"
).count().orderBy("count", ascending=False).show(10, False)
