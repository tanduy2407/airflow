import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from youtube_etl import get_data, query_to_postgres


default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': dt.datetime(2023,3,31),
	'email': ['airflow@example.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
	'youtube_dag',
	default_args=default_args,
	description='Youtube ETL',
	schedule_interval=dt.timedelta(minutes=50),
)

def ETL():
	print('start')
	values = get_data()
	query_to_postgres(keyword='insert', values=values)
	
with dag:
	run_etl = PythonOperator(
		task_id='youtube_etl_final',
		python_callable=ETL,
		dag=dag,
	)

	run_etl