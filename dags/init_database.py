from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from resources.load_static_data import preprocess_district_data
from resources.download import upload_file_to_s3

conf = {
    'bucket_name': 'udacity-dend-capstone-lostkamp',
    's3_prefix_districts': 'district_data'
}


default_args = {
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'start_date': datetime(2022, 3, 1)
}

with DAG(dag_id='init_database',
         description='Run once to create tables and load static data',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=15,
         catchup=False) as dag:

    preprocess_district_data_task = PythonOperator(
        task_id='preprocess_district_data',
        python_callable=preprocess_district_data
    )
    upload_district_file_to_s3_task = PythonOperator(
        task_id='upload_district_file_to_s3',
        python_callable=upload_file_to_s3,
        op_kwargs={'s3_prefix': conf['s3_prefix_districts'],
                   'bucket_name': conf['bucket_name'],
                   'xcom_task_id': 'preprocess_district_data'}
    )
    create_districts_table = PostgresOperator(
        task_id='create_districts_table',
        sql='resources/queries/create_districts_table.sql',
        postgres_conn_id='redshift'
    )
    create_staging_vaccinations_table = PostgresOperator(
        task_id='create_staging_vaccinations_table',
        sql='resources/queries/create_staging_vaccinations_table.sql',
        postgres_conn_id='redshift'
    )
    create_staging_cases_table = PostgresOperator(
        task_id='create_staging_cases_table',
        sql='resources/queries/create_staging_cases_table.sql',
        postgres_conn_id='redshift'
    )

preprocess_district_data_task >> upload_district_file_to_s3_task
