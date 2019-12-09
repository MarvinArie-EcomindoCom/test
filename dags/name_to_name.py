
import os
from os import environ, listdir, path
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pendulum
from ecomindo.spark.libraries.classes import SparkTask

def print_start(ds, **kwargs):
    print("START")

def print_end(**kwargs):
    print("END")

default_args = {
    "owner": "airflow",
    'start_date': datetime(2019, 11, 21,11,24, tzinfo=pendulum.timezone("Asia/Jakarta"))
}

dag = DAG(
    dag_id="Name",
    description="using sparksbmitoperator to hook pyspark transforming supplier data from mongoDb to hive DAG",
    schedule_interval="@once",  
    default_args=default_args
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_start,
		provide_context=True,
    dag=dag
)

spark_task = SparkSubmitOperator(
    task_id='test_nama',
    conn_id='spark_cluster',
    dag=dag,
    executor_memory='1G',
    num_executors=1,
    application=environ['AIRFLOW_HOME']+'/dags/tasks/submit_nama.py',
    conf={'hive.metastore.uris':'thrift://localhost:9083',
					'spark.mongodb.input.uri': 'mongodb://localhost/test.nama',
					'spark.mongodb.output.uri': 'mongodb://localhost/test.nama',
					'spark.hadoop.fs.defaultFS': 'hdfs://192.168.2.58:9000',
					'spark.driver.extraClassPath':environ['SPARK_HOME'] + '/jars'}
)

success_task=SparkSubmitOperator(
    task_id='suc',
    conn_id='spark_cluster',
    dag=dag,
    executor_memory='1G',
    num_executors=1,
    trigger_rule="one_success",
    application=environ['AIRFLOW_HOME']+'/dags/tasks/success.py',
    conf={'hive.metastore.uris':'thrift://localhost:9083',
					'spark.mongodb.input.uri': 'mongodb://localhost/test.nama',
					'spark.mongodb.output.uri': 'mongodb://localhost/test.nama',
					'spark.hadoop.fs.defaultFS': 'hdfs://192.168.2.58:9000',
					'spark.driver.extraClassPath':environ['SPARK_HOME'] + '/jars'}
)

success_task_mysql = BashOperator(
    task_id='successmysql',
    bash_command='python3 /home/willi79/airflow_home/dags/tasks/successmysql.py',
    dag=dag)
    
failed_task_mysql = BashOperator(
    task_id='failedmysql',
    bash_command='python3 /home/willi79/airflow_home/dags/tasks/failedmysql.py',
    dag=dag)

failed_task=SparkSubmitOperator(
    task_id='fail',
    conn_id='spark_cluster',
    dag=dag,
    executor_memory='1G',
    num_executors=1,
    trigger_rule="one_failed",
    application=environ['AIRFLOW_HOME']+'/dags/tasks/failed.py',
    email_on_failure=True,
    email='willigeraldy@gmail.com',
    conf={'hive.metastore.uris':'thrift://localhost:9083',
					'spark.mongodb.input.uri': 'mongodb://localhost/test.nama',
					'spark.mongodb.output.uri': 'mongodb://localhost/test.nama',
					'spark.hadoop.fs.defaultFS': 'hdfs://192.168.2.58:9000',
					'spark.driver.extraClassPath':environ['SPARK_HOME'] + '/jars'}
                
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_end,
    trigger_rule="one_success",
    dag=dag,
)
start_task >> spark_task >> success_task >> success_task_mysql>> end_task
spark_task >> failed_task >> failed_task_mysql>>end_task
