from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

trip_delays_file_path = (
    "../databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
)
airports_na_file_path = (
    "../databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
)
spark = SparkSession.builder.appName("Example").master("local[*]").getOrCreate()
airports_na = spark.read.csv(
    path=airports_na_file_path, header=True, inferSchema=True, sep="\t"
)
departure_delays = spark.read.csv(path=trip_delays_file_path, header=True)

airports_na.createOrReplaceTempView("airports_na")

departure_delays = departure_delays.withColumn(
    "delay", expr("CAST(delay as INT) as delay")
).withColumn("distance", expr("CAST(distance as INT) as distance"))

departure_delays.createOrReplaceTempView("departureDelays")

foo = departure_delays.filter(
    expr(
        """origin == 'SEA' AND destination == 'SFO' and date like '01010%' and delay > 0"""
    )
)
foo.createOrReplaceTempView("foo")

# dense_rank
departure_delays_window = spark.sql(
    """SELECT origin, destination, SUM(delay) AS TotalDelays
    FROM departureDelays
    WHERE origin IN ('SEA', 'SFO', 'JFK')
    AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
    GROUP BY origin, destination;"""
)
departure_delays_window.createOrReplaceTempView("departureDelaysWindow")

spark.sql(
    """
    SELECT origin, destination, TotalDelays, rank
    FROM (
        SELECT origin, destination, TotalDelays,
        dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank
        FROM departureDelaysWindow
    ) t
    WHERE rank <= 3
"""
)
