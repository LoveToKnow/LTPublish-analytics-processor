from logging import ERROR, NullHandler
import pandas as pd
import datetime as dt

from pandas._libs.tslibs import NaT
import mysql.connector as conn

import numpy as np
import google.api_core.retry as retry
from google.cloud import storage

import pdb

def get_connection():
    connection = conn.connect(host="3.87.212.134",
        user="",
        password="",
        database="ltk")
    return connection

def get_records(connection):
    records = None
    cursor = connection.cursor()
#     cursor.execute('''select t.id, t.channel_id, REPLACE(t.title,'"','^>') as title, t.slug, REPLACE(t.url,'\"','^>') as url, 
#     t.type, t.version, t.published_at as og_published_at, t.last_published_at, IFNULL(last_published_at,published_at) as published, t.deleted_at, t.author_user_id
# from title_version t 
# where (updated_at=last_published_at or updated_at=published_at) 
# and updated_at < TIMESTAMP(curdate()) 
# and type not in ('Slideshow','Blogpost');
# # ''')
    # cursor.execute('''select cv.id, cv.vertical_id, cv.title, cv.updated_at, cv.version, cv.hidden, cv.protected ,cv.deleted_at 
    # from channel_version cv
    # ''')
    # cursor.execute('''SELECT user_id, is_active, user_profile.name, date('2021-10-11') as record_date  
    #     FROM ltk.sf_guard_user as user_core join ltk.sf_guard_user_profile as user_profile on user_core.id =user_profile.user_id 
    #     order by is_active;''')
    cursor.execute('''select title_id, type as useful, submitted_at from ltk.useful''')
    results = cursor.fetchall()
    field_names = [i[0] for i in cursor.description]
    records = pd.DataFrame(results,columns=field_names)
    return records

def upload_records_to_cs(records,bucket):
    my_retry = retry.Retry(deadline=60)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    date_pulled = dt.datetime.now().strftime('%Y%m%d')
    blob = bucket.blob(f'{date_pulled}.json')
    blob.upload_from_string(records,retry=my_retry)

def process_tstamp(tstamp):
    if tstamp is np.nan or tstamp is NaT:
        return None
    elif tstamp is None:
        return None
    else:
        if tstamp=="0000-00-00 00:00:00":
            return None
        return str(tstamp).replace(" ","T")


def main():
    ltpublish = None
    try:
        connection = get_connection()
        ltpublish = get_records(connection)
        connection.close()
    except Exception as e:
        print(e)
        raise RuntimeError(e)
    # ltpublish = pd.read_csv('ltpublish_normalised1.csv')
    if ltpublish is not None:
        ltpublish = ltpublish.replace({'\^\>': '"'}, regex=True)
        # ltpublish['og_published_at'] = ltpublish['og_published_at'].apply(process_tstamp)
        # ltpublish['last_published_at'] = ltpublish['last_published_at'].apply(process_tstamp)
        # ltpublish['published'] = ltpublish['published'].apply(process_tstamp)
        # ltpublish['deleted_at'] = ltpublish['deleted_at'].apply(process_tstamp)
        # ltpublish['author_user_id'] = ltpublish['author_user_id'].apply(lambda x: 0 if pd.isna(x) else int(x))

        
        # ltpublish['updated_at'] = ltpublish['updated_at'].apply(process_tstamp)
        # ltpublish['deleted_at'] = ltpublish['deleted_at'].apply(process_tstamp)

        # ltpublish['record_date'] = ltpublish['record_date'].apply(process_tstamp)
        # ltpublish['is_active'] = ltpublish['is_active'].apply(lambda x: True)
        # ltpublish['is_active'] = ltpublish['is_active'].apply(lambda x: False if x is 0 else True)

        ltpublish['submitted_at'] = ltpublish['submitted_at'].apply(process_tstamp)
        json = ltpublish.to_json(
            orient="records",
            lines=True)
        print(len(ltpublish))
        pdb.set_trace()
        upload_records_to_cs(json,'test_the_article_load')

main()