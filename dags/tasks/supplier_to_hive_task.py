from os import environ
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import split, explode 
from ecomindo.spark.libraries.classes import SparkTask

class SupplierToHiveTask(SparkTask):

	def __init__(self):
		super().__init__()

	def run(self):
		self._startSpark(master='spark://192.168.2.58:7077', spark_config= {
								'hive.metastore.uris':'thrift://localhost:9083',
								'spark.mongodb.input.uri': 'mongodb://localhost/test.inventory',
								'spark.mongodb.output.uri': 'mongodb://localhost/test.inventory',
								'spark.hadoop.fs.defaultFS': 'hdfs://192.168.2.58:9000',
								'spark.driver.extraClassPath':environ['SPARK_HOME'] + '/jars',
							 }, log_level= 'INFO')

		self.log.info(f'************************************Start {self.taskName}***********************************') 
		df = self.extract_data()
		df = self.transaform_data(df)
		self.load_data(df)
		self.log.info(f'*******************************Completed {self.taskName}************************************') 
		self.spark.stop()

	def extract_data(self):
		self.log.info('extract data')
		return self.spark.read.format('mongo').load()

	def transaform_data(self, df):
		self.log.info('transform_data')
		df.show()
		return df

	def load_data(self, df):
		self.log.info('load_data')
		df = df.select( df['_id.oid'].alias('id'), explode('supplier').alias('supplier')).select('id', 'supplier.name', 'supplier.level')
		self.log.info('segregate supplier data document into dataframe')
		df.show()
		self.log.info('***********************8insert data to hive supplier table*********************')
		df.write.insertInto('tony.supplier',overwrite = False)
		self.log.info('****************result***********************')
		self.spark.sql('select * from tony.supplier').show()
		self.log.info('****************result***********************')

def main():
	job = SupplierToHiveTask()
	job.run()
	job.close()

if __name__ == '__main__':
    main()
