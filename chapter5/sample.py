from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Example").master("local[*]").getOrCreate()
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

spark.sql(
    """SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrenheit FROM tC"""
).show()
