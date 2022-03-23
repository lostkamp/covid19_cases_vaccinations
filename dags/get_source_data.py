from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from resources.download import (get_case_datafile_url, download_case_file,
                                decompress_case_file, upload_file_to_s3)


default_args = {
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'start_date': datetime(2022, 3, 1)
}


def mytest():
    s3 = S3Hook(aws_conn_id='aws_credentials')
    print(s3.list_prefixes(bucket_name='udacity-dend-capstone-lostkamp'))


with DAG(dag_id='get_source_data',
         description='Download newest case and vaccination data and push it to Redshift',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=15,
         catchup=False) as dag:

    get_case_url_task = PythonOperator(
        task_id='get_case_url_task',
        python_callable=get_case_datafile_url
    )
    download_case_file_task = PythonOperator(
        task_id='download_case_file_task',
        python_callable=download_case_file
    )
    decompress_case_file_task = PythonOperator(
        task_id='decompress_case_file_task',
        python_callable=decompress_case_file
    )
    upload_case_file_to_s3_task = PythonOperator(
        task_id='upload_file_to_s3_task',
        python_callable=mytest
    )

get_case_url_task >> download_case_file_task >> decompress_case_file_task >> upload_case_file_to_s3_task
