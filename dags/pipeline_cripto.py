"""
DAG do pipeline de ETL de criptomoedas.
Orquestra, na ordem, as mesmas funções que já validamos em src/:
extract -> load_bronze -> transform -> load_silver -> load_gold.

Graças ao PYTHONPATH configurado no docker-compose.yaml (/opt/airflow/src),
os módulos extract, transform, load e config são encontrados normalmente,
mesmo esse arquivo estando em uma pasta diferente (dags/).
"""

from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import extract_cripto
from transform import transform_silver
from load import load_bronze, load_silver, load_gold


def task_extract(**context):
    """Busca o snapshot da API e passa o resultado para a próxima task via XCom."""
    df_cripto = extract_cripto()
    # XCom não guarda DataFrames diretamente, então converti para um formato serializável
    context["ti"].xcom_push(key="df_cripto", value=df_cripto.to_json())


def task_load_bronze(**context):
    import pandas as pd
    df_json = context["ti"].xcom_pull(key="df_cripto", task_ids="extract")
    df_cripto = pd.read_json(StringIO(df_json))
    df_cripto["data_coleta"] = pd.to_datetime(df_cripto["data_coleta"], unit="ms")
    load_bronze(df_cripto)


def task_transform(**context):
    df_silver = transform_silver()
    context["ti"].xcom_push(key="df_silver", value=df_silver.to_json())


def task_load_silver(**context):
    import pandas as pd
    df_json = context["ti"].xcom_pull(key="df_silver", task_ids="transform")
    df_silver = pd.read_json(StringIO(df_json))
    df_silver["data_coleta"] = pd.to_datetime(df_silver["data_coleta"], unit="ms")
    load_silver(df_silver)


def task_load_gold(**context):
    import pandas as pd
    df_json = context["ti"].xcom_pull(key="df_silver", task_ids="transform")
    df_silver = pd.read_json(StringIO(df_json))
    df_silver["data_coleta"] = pd.to_datetime(df_silver["data_coleta"], unit="ms")
    load_gold(df_silver)


with DAG(
    dag_id="pipeline_cripto",
    description="ETL de criptomoedas: CoinGecko -> Bronze -> Silver -> Gold",
    schedule="@hourly",          # roda de hora em hora
    start_date=datetime(2026, 1, 1),
    catchup=False,               # não roda execuções "atrasadas" retroativas
    tags=["cripto", "etl", "projeto-final"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    load_bronze_task = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load_silver_task = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
    )

    load_gold_task = PythonOperator(
        task_id="load_gold",
        python_callable=task_load_gold,
    )

    # Define a ordem de execução: extract -> bronze -> transform -> silver e gold em paralelo
    extract >> load_bronze_task >> transform >> [load_silver_task, load_gold_task]