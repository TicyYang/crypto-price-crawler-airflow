import requests
from bs4 import BeautifulSoup
import pendulum
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator
from airflow.utils.db import provide_session
from airflow.models import XCom


def check_crypto_names_(config_key:str) -> str:
    """
    For BranchPythonOperator.
    Check if the crypto names on the website are the same as the names in the config.
    :param config_key : used by Variable.get() to retrieve a pre-configured config in Airflow Variable.
    """

    config = Variable.get(key=config_key, deserialize_json=True)
    crawler_config = config["for_crawler"]
    print("-" * 50)
    print(f"Config:")
    [print(f"{k}: {v}") for k, v in crawler_config.items()]
    print("-" * 50)

    url = crawler_config["url"]
    user_agent = crawler_config["user_agent"]
    headers = {"User-Agent": user_agent}
    name_html_class = crawler_config["name_html_class"]
    num_values = crawler_config["num_values"]
    columns = crawler_config["columns"]

    response = requests.get(url=url, headers=headers)
    response.encoding = "utf-8"

    soup = BeautifulSoup(response.text, "html.parser")

    
    # Get crypto name
    names = []
    try:
        find_names = soup.find_all("span", class_=name_html_class)
    except:
        raise ValueError(f"Can't find span which class is \"{name_html_class}\"!")

    if len(find_names) != num_values:
        raise ValueError("Total number of names is not equal to 12!")
    else:
        for i in range(0, num_values):
            names.append(find_names[i].text.upper())

    print(f"names: {names}")
    print(f"columns: {columns}")
    
    # If there any difference between the names found this time and the columns in the config, raise error
    if names == columns:
        return "continue_next_task"
    else:
        return "unexpected_crypto_name"


def crypto_price_crawl_(config_key:str) -> str:
    """
    For PythonOperator.
    Get crypto prices.
    :param config_key : used by Variable.get() to retrieve a pre-configured config in Airflow Variable.
    """

    config = Variable.get(key=config_key, deserialize_json=True)
    crawler_config = config["for_crawler"]
    print("-" * 50)
    print(f"Config:")
    [print(f"{k}: {v}") for k, v in crawler_config.items()]
    print("-" * 50)

    url = crawler_config["url"]
    user_agent = crawler_config["user_agent"]
    headers = {"User-Agent": user_agent}
    selector_template_price = crawler_config["selector_template_price"]
    num_values = crawler_config["num_values"]


    fetch_dt = pendulum.now().strftime("%Y-%m-%d %H:%M:%S")

    response = requests.get(url=url, headers=headers)
    response.encoding = "utf-8"

    soup = BeautifulSoup(response.text, "html.parser")


    # Get crypto price
    prices = []
    for i in range(1, num_values+1):
        selector = selector_template_price.format(i=i)
        item = soup.select(selector)
        if item:
            price = item[0].text.replace(",", "")
            prices.append(f"\"{price}\"")
        else:
            print(f"No item found for selector: {selector}")
    
    
    data = [f"\"{fetch_dt}\""] + prices
    data_to_insert = (", ").join(data)
    print(f"data_to_insert: {data_to_insert}")

    return data_to_insert


def insert_into_mysql_(config_key:str, sql_cmd_key:str, **context) -> None:
    """
    For PythonOperator.
    Insert the data retrieved through xcom into MySQL.
    :param config_key : used by Variable.get() to retrieve a pre-configured config in Airflow Variable.
    :param sql_cmd_key: used by Variable.get() to retrieve a pre-configured SQL command in Airflow Variable.
    """
    
    config = Variable.get(key=config_key, deserialize_json=True)
    print("-" * 50)
    print(f"Config:")
    [print(f"{k}: {v}") for k, v in config.items()]
    print("-" * 50)

    mysql_conn_id = config["mysql_conn_id"]
    db = config["db"]
    table = config["table"]

    
    ti = context["ti"]
    data_to_insert = ti.xcom_pull(task_ids="crypto_price_crawl")

    sql_cmd = Variable.get(key=sql_cmd_key)
    sql_cmd = sql_cmd.format(db=db, table=table, data_to_insert=data_to_insert)
    print(f"sql_cmd: {sql_cmd}")

    insert_into = SQLExecuteQueryOperator(
        task_id="insert_into", 
        conn_id=mysql_conn_id,
        sql=sql_cmd
    )
    insert_into.execute(dict())


def task_failed_alarm_(mail_recipient:list, **context) -> None:
    """
    For PythonOperator.
    Send an alarm email
    :param mail_recipient: Mail recipients of alarm email.
    """

    dag_run = context['dag_run']
    failed_ti_lst = []
    for ti in dag_run.get_task_instances():
        if ti.state == "failed":
            str_task_id = f"task_id: {ti.task_id}"
            str_exec_date = f"execution_date: {ti.execution_date.astimezone(pendulum.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S.%f')}"
            failed_ti = f"{str_task_id}<br>{str_exec_date}"
            
            failed_ti_lst.append(failed_ti)
    
    email_content = '<br>'.join(failed_ti_lst)
    print(f"email_content: {email_content}")
    
    dag_id = context['dag'].dag_id
    send_email = EmailOperator(
        task_id='send_email',
        to=mail_recipient,
        subject=f"Airflow DAG: {dag_id} task failed",
        html_content=email_content + "<br>" + "<a href='http://192.168.65.134:8080/dags/" + dag_id + "'>Web UI url</a>"
    )
    send_email.execute(dict())


@provide_session
def cleanup_xcom_(dag_id, session=None) -> None:
    """
    For PythonOperator.
    Delete all xcom of this DAG.
    """
    
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()