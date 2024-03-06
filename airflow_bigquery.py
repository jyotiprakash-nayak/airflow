
## airflow dag having tasks  start,  biguerysql execution, end  running every hour daily
##Set up in the Airflow UI under Admin -> Connections

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hourly_bigquery_dag',
    default_args=default_args,
    description='Airflow DAG to run BigQuery SQL hourly',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 6),
    catchup=False
)

start_task = DummyOperator(
    task_id='start',
    dag=dag
)

end_task = DummyOperator(
    task_id='end',
    dag=dag
)

bigquery_sql = """
SELECT *
FROM `your_project_id.your_dataset.your_table`
WHERE date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
"""


bigquery_sql_execution = BigQueryExecuteQueryOperator(
    task_id='bigquery_sql_execution',
    sql=bigquery_sql,
    use_legacy_sql=False,
    bigquery_conn_id='your_bigquery_connection_id',
    dag=dag
)

start_task >> bigquery_sql_execution >> end_task
