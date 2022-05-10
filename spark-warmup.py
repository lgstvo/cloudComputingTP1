from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs:/user/luissilva/hello.txt")
lines = rdd.count()
outrdd = sc.parallelize([lines])
# The following will fail if the output directory exists:
outrdd.saveAsTextFile("hdfs:/user/luissilva/hello-linecount-submit")
sc.stop()