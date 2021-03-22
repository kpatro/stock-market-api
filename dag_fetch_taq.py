import json
import glob
import requests
from time import sleep
import AlpacaTAQ
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

from AlpacaTAQ import Alpaca

API_KEY = "PKLLES9GI1LMTF45SWKZ"
SECRET_KEY = "1rlWRjrT0qdHereRiVo3QD1zDb8yTjD0T35ZkMhL"


def trades():
    trd = Alpaca(client_id=API_KEY, client_secret=SECRET_KEY)
    trd.trades()


def bars():
    br = Alpaca(client_id=API_KEY, client_secret=SECRET_KEY)
    br.bars()


def quotes():
    qt = Alpaca(client_id=API_KEY, client_secret=SECRET_KEY)
    qt.quotes()


default_args = {
    "owner": "Kanhu C Patro",
    "email": "kanhucharan73@gmail.com",
    "start_date": datetime(2020, 2, 21),
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag_sm = DAG(
    dag_id="StockMarketAPI",
    default_args=default_args,
    schedule_interval="@daily",
    tags=['SpringBoard_Capstone']
)

# Step:1 Check Alpaca API
task_check_api = HttpSensor(
    task_id="is_api_available",
    method="GET",
    http_conn_id="url_alpaca",  # configuration on Airflow
    endpoint="",  # authentication
    headers=None,
    request_params=None,
    response_check=lambda response: "OK" in response.text,
    poke_interval=5,
    timeout=5,
    dag=dag_sm
)

# Step 2: Get Trades, Quotes and Bars if Avaiolable
task_bars = PythonOperator(
    task_id="bars",
    python_callable=bars,
    op_kwargs={"API_KEY": f"{API_KEY}", "SECRET-KEY": f"{SECRET_KEY}"},
    dag=dag_sm
)

# Step 2: Get Trades, Quotes and Bars if Avaiolable
task_quotes = PythonOperator(
    task_id="quotes",
    python_callable=quotes,
    op_kwargs={"API_KEY": f"{API_KEY}", "SECRET-KEY": f"{SECRET_KEY}"},
    dag=dag_sm
)

# Step 2: Get Trades, Quotes and Bars if Avaiolable
task_trades = PythonOperator(
    task_id="trades",
    python_callable=trades(),
    op_kwargs={"API_KEY": f"{API_KEY}", "SECRET-KEY": f"{SECRET_KEY}"},
    dag=dag_sm
)

task_check_api >> task_trades
task_check_api >> task_quotes
task_check_api >> task_bars
