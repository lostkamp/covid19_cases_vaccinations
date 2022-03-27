import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from resources.download import (get_case_datafile_url, download_case_file,
                                decompress_case_file, upload_file_to_s3)

conf = {
    'repo_url_case': 'https://storage.googleapis.com/brdata-public-data/rki-corona-archiv/2_parsed/index.html',  # NOQA
    'repo_url_vaccinations': 'https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/master/Archiv/',  # NOQA
    'filename_template_vaccinations': '{DATE}_Deutschland_Landkreise_COVID-19-Impfungen.csv',  # NOQA
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

with DAG(dag_id='get_source_data_v3',
         description='Download newest case and vaccination data and push it to Redshift',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=15,
         catchup=False) as dag:

    date = '2021-08-07'  # TODO: parametrize

    get_case_url_task = PythonOperator(
        task_id='get_case_url',
        python_callable=get_case_datafile_url,
        op_kwargs={'date': date,
                   'repo_url': conf['repo_url_case']}
    )
    download_case_file_task = PythonOperator(
        task_id='download_case_file',
        python_callable=download_case_file
    )
    decompress_case_file_task = PythonOperator(
        task_id='decompress_case_file',
        python_callable=decompress_case_file,
        op_kwargs={'date': date}
    )
    upload_case_file_to_s3_task = PythonOperator(
        task_id='upload_case_file_to_s3',
        python_callable=upload_file_to_s3,
        op_kwargs={'s3_prefix': conf['s3_prefix_case'],
                   'bucket_name': conf['bucket_name'],
                   'xcom_task_id': 'decompress_case_file',
                   'dry_run': True}
    )
    filename = conf['filename_template_vaccinations'].format(DATE=date)
    download_vaccination_file_task = BashOperator(
        task_id='download_vaccination_file',
        bash_command="""
        cd "$FOLDER";
        curl -L $URL > $LOCAL_FNAME;
        echo $LOCAL_FNAME;
        """,
        env={'FOLDER': os.getcwd(),
             'URL': conf['repo_url_vaccinations'] + filename,
             'LOCAL_FNAME': f'{date}.csv'}
    )
    upload_vaccination_file_to_s3_task = PythonOperator(
        task_id='upload_vaccination_file_to_s3',
        python_callable=upload_file_to_s3,
        op_kwargs={'s3_prefix': conf['s3_prefix_vaccinations'],
                   'bucket_name': conf['bucket_name'],
                   'xcom_task_id': 'download_vaccination_file'}
    )
    load_staging_cases_task = S3ToRedshiftOperator(
        task_id='load_staging_cases',
        s3_bucket=conf['bucket_name'],
        s3_key=f'{conf["s3_prefix_case"]}/{date}.ndjson',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        schema='PUBLIC',
        table='staging_cases',
        copy_options=["json 'auto ignorecase'"],
        method='REPLACE',
    )
    load_staging_vaccinations_task = S3ToRedshiftOperator(
        task_id='load_staging_vaccinations',
        s3_bucket=conf['bucket_name'],
        s3_key=f'{conf["s3_prefix_vaccinations"]}/{date}.csv',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        schema='PUBLIC',
        table='staging_vaccinations',
        copy_options=['IGNOREHEADER 1', "delimiter ','"],
        method='REPLACE',
    )
    create_cases_table_task = PostgresOperator(
        task_id='create_cases_table',
        sql='resources/queries/create_cases_table.sql',
        postgres_conn_id='redshift'
    )
    create_vaccinations_table_task = PostgresOperator(
        task_id='create_vaccinations_table',
        sql='resources/queries/create_vaccinations_table.sql',
        postgres_conn_id='redshift'
    )


get_case_url_task >> download_case_file_task >> decompress_case_file_task >> upload_case_file_to_s3_task  # NOQA
download_vaccination_file_task >> upload_vaccination_file_to_s3_task

upload_case_file_to_s3_task >> load_staging_cases_task
upload_vaccination_file_to_s3_task >> load_staging_vaccinations_task

load_staging_cases_task >> create_cases_table_task
load_staging_vaccinations_task >> create_vaccinations_table_task
