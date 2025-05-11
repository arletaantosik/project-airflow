from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO

def _get_stock_prices(url, symbol):
    import requests
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0]) # first item in array

def _store_prices(stock):
    minio = BaseHook.get_connection('minio') # we are creating a service connection in airflow UI, in extra we need to add: { "endpoint_url": "http://minio:9000" } and install minio in docker
    client = Minio( #minio is local alternative to AWS S3 bucket, for development or on-premise
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock) # transforms to dictionary
    symbol = stock['meta']['symbol']  
    data = json.dumps(stock, ensure_ascii=False).encode('utf8') # convert it back to string
    objw = client.put_object( # new file to store in a bucket
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json', #symbol of a company
        data=BytesIO(data), #binarne, to store data in RAM not on disk
        length=len(data) # how bytes has the file (storage)
    )
    return f'{objw.bucket_name}/{symbol}'
