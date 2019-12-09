from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from datetime import datetime

spark = SparkSession \
.builder \
.appName("Success") \
.enableHiveSupport() \
.master("spark://192.168.2.58:7077")\
.config("spark.hadoop.fs.defaultFS","hdfs://192.168.2.58:9000")\
.config("spark.hive.metastore.uris", "thrift://localhost:9083")\
.config("spark.driver.extraClassPath","/home/willi79/BTPN/spark-2.4.4-bin-hadoop2.7/jars")\
.getOrCreate()

date1=datetime.now()
date1=date1.strftime('%Y-%m-%d %H:%M:%S')
s='failed'
spark.sql("insert into coba.status values('"+date1+"','"+s+"')")


