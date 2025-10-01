from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("spark://spark:7077") \
    .getOrCreate()

data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()
