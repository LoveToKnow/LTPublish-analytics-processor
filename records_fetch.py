import io
import base64
from logging import ERROR, NullHandler
import pandas as pd
import datetime as dt
import requests as req
from requests.auth import HTTPBasicAuth
from pandas._libs.tslibs import NaT
import google.api_core.retry as retry
from google.cloud import storage
from google.cloud import secretmanager


def get_records(url):
    uname = secretmanager.SecretManagerServiceClient().access_secret_version(request={"name": 'projects/365738853366/secrets/ltpublish_uname/versions/1'}).payload.data.decode("UTF-8")
    pwd = secretmanager.SecretManagerServiceClient().access_secret_version(request={"name": 'projects/365738853366/secrets/ltpublish_pwd/versions/2'}).payload.data.decode("UTF-8")
    res = req.get(url,auth=HTTPBasicAuth(uname,pwd))
    if res.status_code == 200:
        if len(res.text) > 0:
            dframe = pd.read_csv(io.StringIO(res.text),sep='\t')
        else:
            dframe = None
        return dframe
    else:
        raise RuntimeError('Request to fetch log file failed')


def upload_records_to_cs(report_name,records,bucket):
    my_retry = retry.Retry(deadline=60)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    date_pulled = dt.datetime.now().strftime('%Y%m%d')
    blob = bucket.blob(f'{report_name}_{date_pulled}.json')
    blob.upload_from_string(records,retry=my_retry)

def process_tstamp(tstamp):
    if pd.isna(tstamp) or tstamp is NaT:
        return None
    elif tstamp is None:
        return None
    else:
        if tstamp=="0000-00-00 00:00:00":
            return None
        return str(tstamp).replace(" ","T")

def process_article_history(dframe):
    dframe = dframe.replace({'\^\>': '"'}, regex=True)
    dframe['og_published'] = dframe['og_published'].apply(process_tstamp)
    dframe['last_published_at'] = dframe['last_published_at'].apply(process_tstamp)
    dframe['published'] = dframe['published'].apply(process_tstamp)
    dframe['deleted_at'] = dframe['deleted_at'].apply(process_tstamp)
    dframe.rename(columns={'og_published': 'og_published_at'},inplace=True)
    dframe['author_user_id'] = dframe['author_user_id'].apply(lambda x: 0 if pd.isna(x) else int(x))
    return dframe

def process_channel_history(dframe):
    dframe['updated_at'] = dframe['updated_at'].apply(process_tstamp)
    dframe['deleted_at'] = dframe['deleted_at'].apply(process_tstamp)
    return dframe

def process_author_details(dframe):
    dframe['record_date'] = dframe['record_date'].apply(process_tstamp)
    dframe['is_active'] = dframe['is_active'].apply(lambda x: False if x is 0 else True)
    return dframe

def process_useful_article_history(dframe):
    dframe['submitted_at'] = dframe['submitted_at'].apply(process_tstamp)
    return dframe



def main(event,context):
    process_type=base64.b64decode(event['data']).decode('utf-8')
    ltpublish = None
    url = f'http://prod1.lovetoknow.com/{process_type}.log'
    ltpublish = get_records(url)
    if ltpublish is not None:
        if process_type=="yesterday_article_history":
            ltpublish = process_article_history(ltpublish)
        elif process_type=="yesterday_channel_history":
            ltpublish = process_channel_history(ltpublish)
        elif process_type=="current_author_status":
            ltpublish = process_author_details(ltpublish)
        elif process_type=="yesterday_useful_article_history":
            ltpublish = process_useful_article_history(ltpublish)
        json = ltpublish.to_json(
            orient="records",
            lines=True)
        upload_records_to_cs(process_type,json,'ltpublish-articles-history')
