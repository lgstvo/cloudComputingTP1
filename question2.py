from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("yarn") \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.csv("hdfs:/datasets/covid")
#df.createOrReplaceTempView("DATA")
#df2 = spark.sql("SELECT * from DATA as d where d.")
df_columns = df.columns
df_columns.saveAsTextFile("hdfs:/user/luissilva/df_columns")
sc.stop()