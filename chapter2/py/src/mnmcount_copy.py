from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName("MnMCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
mnm_df = spark.read.csv(path="./data/mnm_dataset.csv", header=True, inferSchema=True)
count_mnm_df: DataFrame = (
    mnm_df.select("State", "Color", "Count")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)
count_mnm_df.show(n=60, truncate=False)
print(f"Total Rows {count_mnm_df.count()}")

ca_count_mnm_df: DataFrame = (
    mnm_df.select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)
ca_count_mnm_df.show(n=10, truncate=False)
