from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_redshift():
    hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    result = hook.get_first("SELECT current_date;")
    print(f"Redshift Query Result: {result}")


with DAG(
    dag_id='test_redshift_connection',
    start_date=datetime(2026, 2, 7),
    schedule_interval=None,
    catchup=False
) as dag:
    test_task = PythonOperator(
        task_id='check_connection',
        python_callable=test_redshift
    )
