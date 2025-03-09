#Sample With AKV and Multi-Steps
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow import DAG
import logging

def step_one():
    logger = logging.getLogger(__name__)
    logger.info("Step One Complete")

def step_two():
    logger = logging.getLogger(__name__)
    logger.info("Step Two Complete")

def step_three():
    logger = logging.getLogger(__name__)
    logger.info("Step Three Complete")

def step_four():
    logger = logging.getLogger(__name__)
    logger.info("Step Four Complete")

def retrieve_variable_from_akv():
    variable_value = Variable.get("sample-variable")
    logger = logging.getLogger(__name__)
    logger.info(variable_value)

with DAG(
   "Multi-Step Example",
   default_args={
       "depends_on_past": False,
       "email": ["airflow@example.com"],
       "email_on_failure": False,
       "email_on_retry": False,
       "retries": 1,
       "retry_delay": timedelta(minutes=5),
    },
   description="This DAG shows how to use Azure Key Vault and create multiple branching tasks",
   schedule_interval=timedelta(days=1),
   start_date=datetime(2025, 3, 8),
   catchup=False,
   tags=["example"],
) as dag:

    get_variable_task = PythonOperator(
        task_id="get_variable",
        python_callable=retrieve_variable_from_akv,
    )

    step_one_task = PythonOperator(
        task_id="step_one",
        python_callable=step_one
    )
    step_two_task = PythonOperator(
        task_id="step_two",
        python_callable=step_two
    )
    step_three_task = PythonOperator(
        task_id="step_three",
        python_callable=step_three
    )
    step_four_task = PythonOperator(
        task_id="step_four",
        python_callable=step_four
    )
    get_variable_task.set_downstream([step_one_task])
    step_one_task.set_downstream([step_two_task])
    step_two_task.set_downstream([step_three_task, step_four_task])

get_variable_task
