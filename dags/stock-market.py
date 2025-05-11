from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],

)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
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
    
    is_api_available() >> get_stock_prices # to run task
        

stock_market()
    
