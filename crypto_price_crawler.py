from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
from functions.crypto_price_crawler_func import (
    check_crypto_names_,
    crypto_price_crawl_,
    insert_into_mysql_,
    check_failed_,
    task_failed_alarm_,
    cleanup_xcom_
)


# ----------Parameters for this dag----------
mail_recipient = ["sirius1993@gmail.com"]
dag_id = "crypto_price_crawler"
dag_time = "*/10 * * * *"
local_tz = pendulum.timezone("Asia/Taipei")
default_args = {
    "owner": "Ticy",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 21, tzinfo=local_tz),
    "email": mail_recipient,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(seconds=3600),
    "pool": f"{dag_id}_pool",
    "queue": "default"
}
# ----------Parameters for this dag----------


with DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule=dag_time,
    max_active_runs=1
) as dag:
    
    # key for Variable.get() in functions
    config_key = "crypto_price_crawler_config"
    sql_cmd_key = "crypto_price_crawler_sql"
    
    # variables for HttpSensor
    http_conn_id = "yahoo_stock"
    endpoint = "{{ var.json.crypto_price_crawler_config.for_http_sensor.endpoint }}"
    
    # ---------------------------------------tasks---------------------------------------
    start = EmptyOperator(task_id="start")


    check_website_health = HttpSensor(
        task_id="check_website_health",
        http_conn_id=http_conn_id,
        endpoint=endpoint,
        method="GET",
        mode="reschedule",
        poke_interval=10,
        timeout=300,
        soft_fail=True
    )
    

    check_crypto_names = BranchPythonOperator(
        task_id="check_crypto_names",
        python_callable=check_crypto_names_,
        op_kwargs={"config_key": config_key}
    )


    continue_next_task = EmptyOperator(task_id="continue_next_task")

    unexpected_crypto_name = EmptyOperator(task_id="unexpected_crypto_name")

    
    crypto_price_crawl = PythonOperator(
        task_id="crypto_price_crawl",
        python_callable=crypto_price_crawl_,
        op_kwargs={"config_key": config_key}
    )


    insert_into_mysql = PythonOperator(
        task_id="insert_into_mysql",
        python_callable=insert_into_mysql_,
        op_args=[config_key, sql_cmd_key]
    )

    
    check_failed = BranchPythonOperator(
        task_id="check_failed",
        python_callable=check_failed_,
        trigger_rule="all_done"
    )


    task_failed_alarm = PythonOperator(
        task_id="task_failed_alarm",
        python_callable=task_failed_alarm_,
        op_kwargs={"mail_recipient": mail_recipient}
    )


    all_success = EmptyOperator(task_id="all_success")


    cleanup_xcom = PythonOperator(
        task_id="cleanup_xcom",
        python_callable=cleanup_xcom_,
        op_kwargs={"dag_id": dag_id}
    )


    end = EmptyOperator(task_id="end")
    # ---------------------------------------tasks---------------------------------------


start >> check_website_health >> check_crypto_names >> [continue_next_task, unexpected_crypto_name]
continue_next_task >> crypto_price_crawl >> insert_into_mysql >> check_failed >> [task_failed_alarm, all_success]
all_success >> cleanup_xcom >> end