from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio( #minio is local alternative to AWS S3 bucket, for development or on-premise
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client
    
def _get_stock_prices(url, symbol):
    import requests
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0]) # first item in array

def _store_prices(stock):
    client = _get_minio_client()
   
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock) # transforms to dictionary
    symbol = stock['meta']['symbol']  
    data = json.dumps(stock, ensure_ascii=False).encode('utf8') # convert it back to string
    objw = client.put_object( # new file to store in a bucket
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json', #symbol of a company
        data=BytesIO(data), #binarne, to store data in RAM not on disk
        length=len(data) # how bytes has the file (storage)
    )
    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path): #looking for existing .csv files
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('The csv file does not exist')
    
