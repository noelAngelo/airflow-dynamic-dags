from airflow.decorators import task, dag, task_group
from airflow.hooks.base import BaseHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime
import requests
from typing import Optional


# Source config
SOURCE_NAME: str = "fleetsu"

# DAG config -- default
DAG_OWNER: str = "airflow"
DAG_START_DATE_YEAR: int = 2023
DAG_START_DATE_MONTH: int = 8
DAG_START_DATE_DAY: int = 15
DAG_RETRIES: int = 1
DAG_TAGS: list = [SOURCE_NAME].extend("['bronze']")
DAG_SCHEDULE_INTERVAL: str = "@daily"
DAG_CATCHUP: bool = False

# Task config -- Databricks
DATABRICKS_CONN_ID: str = "databricks_default"
DATABRICKS_JOB_NAME: str = f'bronze_{SOURCE_NAME}_ingestion_workflow'
DATABRICKS_JOB_ID: Optional[int] = None
DATABRICKS_NOTEBOOK_PARAMS: Optional[dict] = None
DATABRICKS_POLL_PERIOD: int = 30
DATABRICKS_RETRY_LIMIT: int = 3
DATABRICKS_RETRY_DELAY: int = 1

# Task config -- dbt
DBT_CLOUD_CONN_ID = "dbt_cloud_default"
DBT_JOB_NAME: str = f'bronze_{SOURCE_NAME}_processing_job'
DBT_JOB_ID: Optional[int] = None
DBT_ACCOUNT_ID: int = 3405
DBT_PROJECT_ID: int = 2780
DBT_TRIGGER_REASON: str = "Triggered via Airflow"

# Task config -- MS Teams
MSTEAMS_CONN_ID = "msteams_default"


# -- DO NOT EDIT PAST HERE --


class DbtCloudHook(BaseHook):
    """
    Airflow Hook for interacting with the dbt Cloud API to list jobs.
    """

    def __init__(self, dbt_cloud_conn_id):
        super().__init__()
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.conn = self.get_connection(self.dbt_cloud_conn_id)
        self.base_url = self.conn.host.rstrip('/')
        self.token = self.conn.password  # Assuming token-based authentication

    def _build_headers(self):
        return {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json',
        }

    def list_jobs(self, account_id: int):
        endpoint = f'/accounts/{account_id}/jobs/'
        url = f'{self.base_url}{endpoint}'
        headers = self._build_headers()

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_array = response.json()  # dbt API returns an array
            response_dict = response_array[0]
            if response_dict['status']['code'] == 200:
                return response_dict['data']

        else:
            response.raise_for_status()

    def get_job_id(self, name: str, account_id: int, project_id: int):
        jobs = self.list_jobs(account_id)
        results = list(
            filter(lambda job:
                   job['name'] == name and
                   job['account_id'] == account_id and
                   job['project_id'] == project_id,
                   jobs))
        if results:
            return results[0]


class MSTeamsWebhookHook(BaseHook):
    """
    Airflow Hook for sending messages to a Microsoft Teams channel using a webhook.
    """

    def __init__(self, teams_conn_id='msteams_default'):
        super().__init__()
        self.teams_conn_id = teams_conn_id
        self.conn = self.get_connection(self.teams_conn_id)
        self.webhook_url = self.conn.host.rstrip('/')  # Assuming the connection holds the webhook URL

    def send_message(self, databricks_workflow_id, dbt_job_id, dbt_project_id):
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.5",
                        "msteams": {
                            "width": "Full"
                        },
                        "body": [
                            # HEADING
                            {
                                "type": "TextBlock",
                                "text": f"{SOURCE_NAME} Orchestration Results",
                                "wrap": "true",
                                "size": "large",
                                "style": "heading"
                            },

                            # COLUMN SET
                            {
                                "type": "ColumnSet",
                                "columns": [
                                    {
                                        "type": "Column",
                                        "width": "auto",
                                        "items": [
                                            {
                                                "type": "Image",
                                                "size": "large",
                                                "url": "https://static.wixstatic.com/media/3fe6c2_2fa1a0bab851460db88822f76b6b88b4~mv2.png/v1/fill/w_1000,h_464,al_c,q_90,usm_0.66_1.00_0.01/3fe6c2_2fa1a0bab851460db88822f76b6b88b4~mv2.png",
                                                "altText": "Airflow logo"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Column",
                                        "width": "stretch",
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "DAG Run",
                                                "horizontalAlignment": "right",
                                                "isSubtle": "true",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "COMPLETE",
                                                "horizontalAlignment": "right",
                                                "spacing": "none",
                                                "size": "large",
                                                "color": "good",
                                                "wrap": "true"
                                            }
                                        ]
                                    }
                                ]
                            },

                            # COLUMN SET
                            {
                                "type": "ColumnSet",
                                "separator": "true",
                                "spacing": "medium",
                                "columns": [
                                    {
                                        "type": "Column",
                                        "width": "stretch",
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Bronze Build",
                                                "isSubtle": "true",
                                                "weight": "bolder",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": f"Databricks - {SOURCE_NAME.title()}",
                                                "spacing": "small",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": f"dbt - {SOURCE_NAME.title()}",
                                                "spacing": "small",
                                                "wrap": "true"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Column",
                                        "width": "auto",
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Status",
                                                "horizontalAlignment": "right",
                                                "isSubtle": "true",
                                                "weight": "bolder",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Success",
                                                "horizontalAlignment": "right",
                                                "spacing": "small",
                                                "color": "good",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": "Success",
                                                "horizontalAlignment": "right",
                                                "spacing": "small",
                                                "color": "good",
                                                "wrap": "true"
                                            }
                                        ]
                                    }
                                ]
                            },

                            # COLUMN SET
                            {
                                "type": "ColumnSet",
                                "spacing": "medium",
                                "separator": "true",
                                "columns": [
                                    {
                                        "type": "Column",
                                        "width": 1,
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "Databricks Workflow",
                                                "isSubtle": "true",
                                                "weight": "bolder",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": f"{databricks_workflow_id}",
                                                "spacing": "small",
                                                "wrap": "true"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Column",
                                        "width": 1,
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "dbt Job",
                                                "isSubtle": "true",
                                                "horizontalAlignment": "center",
                                                "weight": "bolder",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": f"{dbt_job_id}",
                                                "weight": "bolder",
                                                "horizontalAlignment": "center",
                                                "spacing": "small",
                                                "wrap": "true"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "Column",
                                        "width": 1,
                                        "items": [
                                            {
                                                "type": "TextBlock",
                                                "text": "dbt Project",
                                                "isSubtle": "true",
                                                "horizontalAlignment": "right",
                                                "weight": "bolder",
                                                "wrap": "true"
                                            },
                                            {
                                                "type": "TextBlock",
                                                "text": f"{dbt_project_id}",
                                                "horizontalAlignment": "right",
                                                "weight": "bolder",
                                                "spacing": "small",
                                                "wrap": "true"
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }

        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(self.webhook_url, json=payload, headers=headers)

        if response.status_code == 200:
            return "Message sent successfully to Microsoft Teams."
        else:
            response.raise_for_status()


default_args = {
    'owner': DAG_OWNER,
    'start_date': datetime(DAG_START_DATE_YEAR, DAG_START_DATE_MONTH, DAG_START_DATE_DAY),
    'retries': DAG_RETRIES,
}


@task_group
def dbt_run():
    @task(task_id=f'get_{SOURCE_NAME}_job_id')
    def get_dbt_job_id(name: str, account_id: int, project_id: int):
        hook = DbtCloudHook(dbt_cloud_conn_id=DBT_CLOUD_CONN_ID)
        return hook.get_job_id(name=name, account_id=account_id, project_id=project_id)

    # define Op
    t1 = get_dbt_job_id(name=DBT_JOB_NAME, account_id=DBT_ACCOUNT_ID, project_id=DBT_PROJECT_ID)
    processing_op = DbtCloudRunJobOperator(
        task_id=f'trigger_{SOURCE_NAME}_job',
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=t1,
        account_id=DBT_ACCOUNT_ID,
        trigger_reason=DBT_TRIGGER_REASON)

    # define DAG
    return t1.set_downstream(processing_op)


@task
def send_teams_message(databricks_workflow_id, dbt_job_id, dbt_project_id):
    hook = MSTeamsWebhookHook(teams_conn_id=MSTEAMS_CONN_ID)
    result = hook.send_message(databricks_workflow_id, dbt_job_id, dbt_project_id)
    return result


@dag(dag_id=f'elt_{SOURCE_NAME}_dag',
     schedule_interval=DAG_SCHEDULE_INTERVAL,
     default_args=default_args,
     catchup=DAG_CATCHUP,
     tags=DAG_TAGS)
def elt_dag():
    # define Ops
    t2 = dbt_run()
    t3 = send_teams_message(0, 1, DBT_PROJECT_ID)
    ingestion_op = DatabricksRunNowOperator(
        task_id=f'trigger_{SOURCE_NAME}_workflow',
        job_name=DATABRICKS_JOB_NAME,
        notebook_params=DATABRICKS_NOTEBOOK_PARAMS,
        polling_period_seconds=DATABRICKS_POLL_PERIOD,
        databricks_retry_limit=DATABRICKS_RETRY_LIMIT,
        databricks_retry_delay=DATABRICKS_RETRY_DELAY,
        databricks_conn_id=DATABRICKS_CONN_ID)

    # define DAG
    ingestion_op.set_downstream(t2)  # t2 depends on ingestion_op
    t2 >> t3  # t3 depends on t2


elt_dag()