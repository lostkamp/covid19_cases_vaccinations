import os
from urllib.request import urlretrieve
import lzma
import shutil
import logging
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_case_datafile_url(ds, repo_url):
    from bs4 import BeautifulSoup
    logging.info(f'Getting data URL for: {ds}')
    urlretrieve(repo_url, 'index.html')
    with open('index.html', 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
    links_tags = soup.find_all('a')
    file_urls = list(map(lambda x: x.attrs['href'], links_tags))

    result_list = list(filter(lambda x: ds in x, file_urls))
    if len(result_list) > 1:
        # More than one dataset is available for the requested date.
        # In that case we take the one with the largest timestamp
        result = sorted(result_list)[-1]
    elif len(result_list) == 1:
        result = result_list[0]
    else:
        raise IOError(f'No file found matching {ds}')

    logging.info(f'Found URL: {result}')
    return result


def download_case_file(ti):
    url = ti.xcom_pull(task_ids='get_case_url_task')
    local_fname = os.path.split(url)[-1]
    logging.info(f'Downloading: {url}')
    logging.info(f'Local path: {os.path.join(os.getcwd(), local_fname)}')
    urlretrieve(url, local_fname)
    return local_fname


def decompress_case_file(ti):
    filename_in = ti.xcom_pull(task_ids='download_case_file_task')
    filename_out = os.path.splitext(filename_in)[0]
    with lzma.open(filename_in, 'rb') as fin, open(filename_out, 'wb') as fout:
        shutil.copyfileobj(fin, fout)
    logging.info(f'Decompressed file: {filename_out}')
    return filename_out


def upload_file_to_s3(ti, s3_prefix, bucket_name, delete_local_file=True):
    local_fname = ti.xcom_pull(task_ids='decompress_case_file_task')
    s3 = S3Hook(aws_conn_id='aws_credentials', region_name='eu-west-1')
    s3_key = os.path.join(s3_prefix, local_fname)
    logging.info(f'Uploading to S3 path: s3://{bucket_name}/{s3_key}')
    s3.load_file(local_fname, key=s3_key, bucket_name=bucket_name)
    logging.info('Upload complete.')
    if delete_local_file:
        logging.info(f'Deleting local file: {local_fname}')
        Path(local_fname).unlink()
