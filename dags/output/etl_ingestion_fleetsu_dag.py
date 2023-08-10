from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(dag_id="etl_ingestion_fleetsu_dag",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:
    
    opr_run_now = DatabricksRunNowOperator(
        task_id = "fleetsu_databricks_workflow",
        databricks_conn_id = "databricks_default",
        job_id = "0"
    )
    
    opr_run_now