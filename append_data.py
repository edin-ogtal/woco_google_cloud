import time
import pandas as pd
import urllib.request, json
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound, Conflict

OUTPUT_DATASET = 'aarhus'
OUTPUT_TABLE = 'trafik'
OUTPUT_BUCKET = 'aarhus_trafik'

def get_data(url):
    with urllib.request.urlopen(url) as f: 
        data = json.loads(f.read().decode()) 
    return(data)

def get_trafic_data():
    df = pd.DataFrame()

    offset = ''
    base_url = 'https://portal.opendata.dk/api/3/action/datastore_search?{}resource_id=b3eeb0ff-c8a8-4824-99d6-e0a3747c8b0d'
    data_url = base_url.format(offset)

    data = get_data(data_url)
    n_results = data['result']['total']
    df = pd.DataFrame(data['result']['records'])
    #print(n_results)
    i = 100
    if n_results < 100: 
        return(df)
    else:
        while i < n_results:
            next_page_url = base_url.format('offset='+str(i)+'&')
            #print(next_page_url)
            data = get_data(next_page_url)
            df_next_page = pd.DataFrame(data['result']['records'])
            #print(df_next_page.head())
            df = df.append(df_next_page)
            i += 100

        return(df)

def save_df_to_bucket(df, bucket_name, storage_client): 
    # Make buckets 
    try:
        storage_client.create_bucket(bucket_name)
    except Conflict:
        pass
    # Get bucket
    bucket = storage_client.bucket(bucket_name)

    # Save data to bucket
    fn = 'gs://aarhus_trafik/test_new_data.csv' 
    df.to_csv(fn)

    grouped = df.groupby('vehicleCount')

    for count, sub_df in grouped:
        bucket_name = 'vehiclecount-{}'.format(count)
        #print(count)
        time.sleep(2)
        try:
            storage_client.create_bucket(bucket_name)
        except Conflict:
            pass

        # Get bucket
        bucket = storage_client.bucket(bucket_name)

        fn = 'gs://{}/aarhus_data.csv'.format(bucket_name)
    
        sub_df.to_csv(fn)

def save_df_to_table(df, dataset_name, table_name, bq_client):
    # Make table 
    try:
        dataset = bq_client.create_dataset(dataset_name)
    except Conflict:
        pass
    # Get table 
    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

  
    # Append data to table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_APPEND"
    job_config.autodetect = True  
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)


def run_append():

    OUTPUT_DATASET = 'aarhus'
    OUTPUT_TABLE = 'trafik'
    OUTPUT_BUCKET = 'aarhus_trafik'

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    df = get_trafic_data()
    save_df_to_bucket(df, OUTPUT_BUCKET, storage_client)
    save_df_to_table(df, OUTPUT_DATASET, OUTPUT_BUCKET, bq_client)
