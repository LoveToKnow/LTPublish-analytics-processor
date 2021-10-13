## LTPublish data
The following repo stores the function deployed as a google cloud function for the collection and processing of LTPublish data to our analytics db, bigquery.

`records_fetch.py` is resposible for fetching, transforming (the transforms are minimal) and saving the data in google cloud storage for later processing. It is triggered by a GCP scheduler for each type of report we want to fetch 

`gcs_to_bq.py` takes data from gcs to bq. It is also deployed as a cloud function and is triggered by the addition of a file to the google cloud storage bucket used by `records_fetch`. This bucket is `ltpublish-articles-history` which can be found within the `custom-site-analytics` project.

The `local_run.py` script is a means to directly query the LTPublish DB for fetching and pushing/reloading. In order to do this you need to add your own db credentials and set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` with the appropriate GCP access. Normally I would set the variable to the path of a service account json file.

Once `local_run.py` has been executed you can use the `bq` cmd tool to load the output to the relevant table with a command like the following:

```
bq load    --source_format=NEWLINE_DELIMITED_JSON   ltk.channel_history     gs://test_the_article_load/20211013.json
```