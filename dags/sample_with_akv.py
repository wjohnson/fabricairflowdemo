#Sample With AKV
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow import DAG
import logging

def retrieve_variable_from_akv():
    variable_value = Variable.get("sample-variable")
    logger = logging.getLogger(__name__)
    logger.info(variable_value)

with DAG(
   "tutorial",
   default_args={
       "depends_on_past": False,
       "email": ["airflow@example.com"],
       "email_on_failure": False,
       "email_on_retry": False,
       "retries": 1,
       "retry_delay": timedelta(minutes=5),
    },
   description="This DAG shows how to use Azure Key Vault to retrieve variables in Apache Airflow DAG",
   schedule_interval=timedelta(days=1),
   start_date=datetime(2021, 1, 1),
   catchup=False,
   tags=["example"],
) as dag:

    get_variable_task = PythonOperator(
        task_id="get_variable",
        python_callable=retrieve_variable_from_akv,
    )

get_variable_task
