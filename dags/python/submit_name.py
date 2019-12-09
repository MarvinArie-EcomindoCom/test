from pyspark.sql import SparkSession, Row, HiveContext, SQLContext
from pyspark.sql.functions import split, explode
from pyspark import SparkContext, SparkConf
import os


#.config("spark.driver.extraClassPath":"/home/willi79/BTPN/spark-2.4.4-bin-hadoop2.7/jars")\
#.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")\

#warehouseLocation = "hdfs://192.168.2.58:9000/user/hive/warehouse"
#.config("spark.sql.warehouse.dir", warehouseLocation)\
warehouseLocation = "hdfs://192.168.2.58:9000/user/hive/warehouse"
spark = SparkSession \
.builder \
.appName("myAppName") \
.enableHiveSupport() \
.master("mesos://192.168.2.58:5050")\
.config("spark.hadoop.fs.defaultFS","hdfs://192.168.2.58:9000")\
.config("spark.hive.metastore.uris", "thrift://localhost:9083")\
.config("spark.driver.extraClassPath","/home/willi79/BTPN/spark-2.4.4-bin-hadoop2.7/jars")\
.config("spark.mongodb.input.uri", "mongodb://localhost:27017/test.nama") \
.config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.nama") \
.config("spark.sql.warehouse.dir", warehouseLocation)\
.getOrCreate()
print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
df=spark.read.format("mongo").load()
df.printSchema()
name=df.select("name")
name.write.mode("overwrite").saveAsTable("coba.name")


