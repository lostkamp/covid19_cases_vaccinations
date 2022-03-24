import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from resources.download import (get_case_datafile_url, download_case_file,
                                decompress_case_file, upload_file_to_s3)

conf = {
    'repo_url_case': 'https://storage.googleapis.com/brdata-public-data/rki-corona-archiv/2_parsed/index.html',  # NOQA
    'repo_url_vaccinations': 'https://github.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/tree/master/Archiv/',
    'filename_template_vaccinations': '{DATE}_Deutschland_Landkreise_COVID-19-Impfungen.csv?raw=true',
    'bucket_name': 'udacity-dend-capstone-lostkamp',
    's3_prefix_case': 'case_data',
    's3_prefix_vaccinations': 'vaccination_data'
}


default_args = {
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'start_date': datetime(2022, 3, 1)
}

with DAG(dag_id='get_source_data_v2',
         description='Download newest case and vaccination data and push it to Redshift',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=15,
         catchup=False) as dag:

    date = '2021-07-23'

    get_case_url_task = PythonOperator(
        task_id='get_case_url_task',
        python_callable=get_case_datafile_url,
        op_kwargs={'date': date,
                   'repo_url': conf['repo_url_case']}
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
        task_id='upload_case_file_to_s3_task',
        python_callable=upload_file_to_s3,
        op_kwargs={'s3_prefix': conf['s3_prefix_case'],
                   'bucket_name': conf['bucket_name']}
    )
    filename = conf['filename_template_vaccinations'].format(DATE=date)
    download_vaccination_file_task = BashOperator(
        task_id='download_vaccination_file_task',
        bash_command="""
        cd "$FOLDER";
        curl -L $URL > $LOCAL_FNAME;
        echo $LOCAL_FNAME;
        """,
        env={'FOLDER': os.getcwd(),
             'URL': conf['repo_url_vaccinations'] + filename + '?raw=true',
             'LOCAL_FNAME': filename}
    )
    upload_vaccination_file_to_s3_task = PythonOperator(
        task_id='upload_vaccination_file_to_s3_task',
        python_callable=upload_file_to_s3,
        op_kwargs={'s3_prefix': conf['s3_prefix_vaccinations'],
                   'bucket_name': conf['bucket_name']}
    )


get_case_url_task >> download_case_file_task >> decompress_case_file_task >> upload_case_file_to_s3_task
download_vaccination_file_task >> upload_vaccination_file_to_s3_task
