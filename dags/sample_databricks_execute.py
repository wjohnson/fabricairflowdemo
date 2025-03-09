# Sample Databricks Execute
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago


default_args = {
  'owner': 'airflow'
}

with DAG('databricks_dag',
  start_date = days_ago(2),
  schedule_interval = "@daily",
  default_args = default_args
) as dag:

 transform_data = DatabricksRunNowOperator(
    task_id = 'transform_data',
    databricks_conn_id = 'databricks_default',
    job_id ="70735104287385"
  )
