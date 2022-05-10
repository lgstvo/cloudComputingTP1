from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.csv("hdfs:/datasets/covid")

df.saveAsTextFile("hdfs:/user/luissilva/df")
sc.stop()