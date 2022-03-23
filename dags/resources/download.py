import os
from urllib.request import urlretrieve
import lzma
import shutil


REPO_URL = 'https://storage.googleapis.com/brdata-public-data/rki-corona-archiv/2_parsed/index.html'


def get_case_datafile_url(ds):
    from bs4 import BeautifulSoup
    print('Templated date:', ds)
    urlretrieve(REPO_URL, 'index.html')
    with open('index.html', 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
    links_tags = soup.find_all('a')
    file_urls = list(map(lambda x: x.attrs['href'], links_tags))

    result = list(filter(lambda x: ds in x, file_urls))
    if len(result) > 1:
        result = sorted(result)[-1]
    elif len(result) == 1:
        result = result[0]
    else:
        raise IOError(f'No file found matching {ds}')

    return result


def download_case_file(ti):
    url = ti.xcom_pull(task_ids='get_case_url_task')
    local_fname = os.path.split(url)[-1]
    urlretrieve(url, local_fname)
    return local_fname


def decompress_case_file(ti):
    filename_in = ti.xcom_pull(task_ids='download_case_file_task')
    filename_out = os.path.splitext(filename_in)[0]
    with lzma.open(filename_in, 'rb') as fin, open(filename_out, "wb") as fout:
        shutil.copyfileobj(fin, fout)

    return filename_out


def upload_file_to_s3(ti):
    print('Filename:', ti.xcom_pull(task_ids='decompress_case_file_task'))
