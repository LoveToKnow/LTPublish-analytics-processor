from google.cloud import bigquery

def gcs_to_bq(event, context):
    
    uri = f'gs://{event["bucket"]}/{event["name"]}'
    if 'yesterday_article_history' in event["name"]:
        table = 'custom-site-analytics.ltk.article_history'
    elif 'yesterday_channel_history' in event["name"]:
        table = 'custom-site-analytics.ltk.channel_history'
    elif 'current_author_status' in event["name"]:
        table = 'custom-site-analytics.ltk.author_history'
    elif 'yesterday_useful_article_history' in event["name"]:
        table = 'custom-site-analytics.ltk.article_useful_history'
    if table is None:
        raise RuntimeError('No table destination found, please supply in the form: project.dataset.table')
    print(uri)
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    load_job = client.load_table_from_uri(
        uri, table, job_config=job_config
    ) 
    load_job.result()

