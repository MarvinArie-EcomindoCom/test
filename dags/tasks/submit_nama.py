from os import environ
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import split, explode, lit
from ecomindo.spark.libraries.classes import SparkTask
from datetime import datetime


class SupplierToHiveTask(SparkTask):

	def __init__(self):
		super().__init__()

	def run(self):
		self._startSpark(master='local', spark_config= {
								'hive.metastore.uris':'thrift://localhost:9083',
								'spark.mongodb.input.uri': 'mongodb://localhost/test.nama',
								'spark.mongodb.output.uri': 'mongodb://localhost/test.nama',
								'spark.hadoop.fs.defaultFS': 'hdfs://192.168.2.58:9000',
								'spark.driver.extraClassPath':environ['SPARK_HOME'] + '/jars',
							 }, log_level= 'INFO')

		self.log.info(f'***********************************Start {self.taskName}***********************************') 
		df = self.extract_data()
		df = self.transaform_data(df)
		self.load_data(df)
		self.log.info(f'*******************************Completed {self.taskName}************************************') 
		self.spark.stop()

	def extract_data(self):
		self.log.info('extract data')
		return self.spark.read.format('mongo').load()

	def transaform_data(self, df):
		self.log.info('transaform_data')
		df.show()
		return df

	def load_data(self, df):
		self.log.info('load_data')
		df = df.select('name','class')
		date1=datetime.now()
		date1=date1.strftime('%Y-%m-%d')
		df = df.withColumn("dt", lit(datetime.now()))
		self.log.info('segregate supplier data document into dataframe')
		df.show()
		self.log.info('***********************8insert data to hive supplier table*********************')
		df.write.insertInto('coba.name',overwrite = False)
		self.log.info('****************result***********************')
		self.spark.sql('select * from coba.name').show()
		self.log.info('****************result***********************')

def main():
	job = SupplierToHiveTask()
	job.run()
	job.close()

if __name__ == '__main__':
    main()
