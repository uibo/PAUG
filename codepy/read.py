from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadingApp").getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/user/test1/iphone14_cleaning.csv", header = True, multiLine=True, quote='"', escape='"')

df.show()
