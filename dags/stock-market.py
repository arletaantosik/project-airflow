from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator #in requirements.txt to install: apache-airflow-providers-dockers==4.0.0 ===>>> then in console: astro dev restart; astro dev run dags test stock_market 2025-01-01
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv#, BUCKET_NAME

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False, #no backfilling - nie uruchamia z daty z przeszlosci
    tags=['stock_market'],

)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke') #poke_intervals -> asks every 30 seconds if the condition was met, timeout - how long sensor will try, 
    # mode = poke -> sensor is active all time, in "reschedule" is activated every 30 seconds, which the second option is less resource consuming, but cheaper
    def is_api_available() -> PokeReturnValue: #create connection in airflow api - connection type: http, https://query1.finance.yahoo.com/v8/finance/chart/
        import requests
        
        api = BaseHook.get_connection('stock_api') # to connect with airflow
        url = f"{api.host}{api.extra_dejson['endpoint']}" # cause json value in "extra" when creating 'stock_api'
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None # then API available
        return PokeReturnValue(is_done=condition, xcom_value=url) # xcom => to export url to the next task
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL} #dictionary with 2 parameters, url and symbol, which are used in def _get_stock_prices
        # ti.xcom_pull(task_ids="is_api_available" - templating, {{ evaluates only when runs
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'} #only 1 parameters in function
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices', #in docker deskop
        api_version='auto',
        auto_remove='success', #contener will be deleted automatically, if it is ended 'success'
        docker_url='tcp://docker-proxy:2375', #default
        network_mode='container:spark-master', # the same network as spark is using
        tty=True, #to turn terminal on
        xcom_all=False, # it doesn't write the whole output to xcom
        mount_tmp_dir=False, # to not mount temporary directory
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}' #dictionary, xcom from previous task
            #ti. -> task instance
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}' #dictionary
        }
    )
    #create connection for postgres in airflow UI
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File( #which files we want to load in
            path=f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='get_formatted_csv') }}}}", #MinIO path, get_formatted_csv -> is the path to .csv
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata( #metadata object
                schema='public'
            )
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host,
        }
    )
    
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw# to run task
        

stock_market()
    
