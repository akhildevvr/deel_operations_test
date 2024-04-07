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
from validation_tests.deel_operations import balance_validation



var_snowflake_conn_id="snowflake_conn"
def snowflake_env_variables(var_snowflake_conn_id):
    # Connection configure in the Airflow UI
    conn = BaseHook.get_connection(var_snowflake_conn_id)

    profile_vars = {}
    profile_vars['SNOWFLAKE_SVC_USER'] = conn.login
    profile_vars['SNOWFLAKE_SVC_PASSWD'] = conn.password
    profile_vars['SNOWFLAKE_SCHEMA'] = conn.schema
    conn_extras = json.loads(conn.extra)
    profile_vars['SNOWFLAKE_ROLE'] = conn_extras['role']
    profile_vars['SNOWFLAKE_DB'] = conn_extras['database']
    profile_vars['SNOWFLAKE_WAREHOUSE'] = conn_extras['warehouse']
    profile_vars['SNOWFLAKE_ACCOUNT'] = conn_extras['account']

    return profile_vars


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



DBT_DIR = "/Users/ravinda/Documents/deel_operations_test/dags/dbt/deel_operations"
DBT_PROFILES_DIR = "/Users/ravinda/Documents/deel_operations_test/dags/dbt/dbt_profiles"

with DAG(
    dag_id='deel_operations',
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
