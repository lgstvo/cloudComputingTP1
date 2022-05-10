from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("HelloLines") \
    .getOrCreate()

df = spark.read.csv("hdfs:/datasets/covid")
df.printSchema()