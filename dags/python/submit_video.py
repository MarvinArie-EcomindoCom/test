from pyspark.sql import SparkSession, Row, HiveContext, SQLContext
from pyspark.sql.functions import split, explode
from pyspark import SparkContext, SparkConf
import os


#.config("spark.driver.extraClassPath","/home/willi79/BTPN/mongo-spark-connector_2.11-2.4.1.jar:/home/willi79/BTPN/mongo-java-driver-3.10.2.jar").config("spark.executor.extraClassPath","/home/willi79/BTPN/mongo-spark-connector_2.11-2.4.1.jar:/home/willi79/BTPN/mongo-java-driver-3.10.2.jar")\
#.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")\

#warehouseLocation = "hdfs://192.168.2.58:9000/user/hive/warehouse"'
#.config("spark.sql.warehouse.dir", warehouseLocation)\
spark = SparkSession \
.builder \
.appName("myApp") \
.enableHiveSupport() \
.master("spark://192.168.2.58:7077")\
.config("spark.hadoop.fs.defaultFS","hdfs://192.168.2.58:9000")\
.config("spark.hive.metastore.uris", "thrift://localhost:9083")\
.config("spark.mongodb.input.uri", "mongodb://m001-student:m001-mongodb-basics@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/video.movies?replicaSet=Cluster0-shard-0&ssl=true&authSource=admin") \
.config("spark.mongodb.input.readPreference.name", "primaryPreferred")\
.config("spark.mongodb.output.uri", "mongodb://m001-student:m001-mongodb-basics@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/video.movies?replicaSet=Cluster0-shard-0&ssl=true&authSource=admin ") \
.getOrCreate()
print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
df=spark.read.format("mongo").load()
df.printSchema()
film=df.select(df["_id.oid"].alias("id"),"title","year","genre","runtime").limit(10)
film.write.mode("overwrite").saveAsTable("coba.video")


