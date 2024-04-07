from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import os
import json
from datetime import datetime, timedelta
from common.scripts.slack_connect import *
from include.env_variables import snowflake_env_variables
from validation_tests.deel_operations import balance_validation







# Defining success & failure callback functions
def success_callback(context):
    success_email(context, args['EMAILS'])


def failure_callback(context):
    slack_notification(context)


# Default settings applied to all tasks
default_args = {
    'owner': 'akhil',
    'depends_on_past': False,
    'on_failure_callback': failure_callback,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}



DBT_DIR = "/usr/local/airflow/dags/dbt/adp-enterprise"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/dbt_profiles"
DBT_MODEL = "--model tag:tflex_usage_dd_fct"
DBT_MONITORING_MODEL = "--model tag:token_monitoring"
FULL_REFRESH_FLAG = f"--full-refresh " if Variable.get("full_refresh", default_var="False") == "True" else ""

with DAG(
    dag_id='tflex_usage_dd_fct',
    start_date=datetime(2022, 2, 20),
    max_active_runs=3,
    schedule_interval='0 8 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    transform = BashOperator(
            task_id='transform_models',
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt run --profiles-dir {DBT_PROFILES_DIR} --model tag:transform
            """,
            env=snowflake_env_variables(var_snowflake_conn_id),
            dag=dag
            
        )
    
    tests = BashOperator(
            task_id='test_models',
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt test --profiles-dir {DBT_PROFILES_DIR}
            """,
            env=snowflake_env_variables(var_snowflake_conn_id),
            dag=dag
        )

    run_validation = PythonOperator(
        dag=dag,
        task_id = 'validation_checks',
        python_callable = balance_validation,
        retries=0
        )

    publish = BashOperator(
            task_id='publish_models',
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt run --profiles-dir {DBT_PROFILES_DIR} --model tag:publish
            """,
            env=snowflake_env_variables(var_snowflake_conn_id),
            dag=dag
            
        )
    

    success = DummyOperator(task_id='success_email',
                            on_success_callback=success_callback
                            )

transform >> tests >> run_validation >> publish >> success
