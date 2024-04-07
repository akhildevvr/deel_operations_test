
from datetime import timedelta, date
import os
import yaml
import inspect
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

prev_dt = (date.today() - timedelta(days=1)).strftime('%Y%m%d')



def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}
            *Start Date*: {start_date}
            *Error Message*: {error_msg}
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            start_date=context.get("task_instance").start_date,
            error_msg = context.get('exception'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_connection',
        message=slack_msg)
    return failed_alert.execute(context=context)



def slack_notification_validation(context, reason):
    slack_msg = """
            :bc-danger: Validation Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}
            *Start Date*: {start_date}
            *Alert Message*: {error_msg}
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            start_date=context.get("task_instance").start_date,
            error_msg = reason,
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_connection',
        message=slack_msg)
    return failed_alert.execute(context=context)
