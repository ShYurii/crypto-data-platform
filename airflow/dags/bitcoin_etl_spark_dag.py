from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import logging

logger = logging.getLogger(__name__)

# ----------------------------
# EXTRACT
# ----------------------------
def extract_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    price = data["bitcoin"]["usd"]

    logger.info(f"[EXTRACT] received price = {price}")

    return price


# ----------------------------
# TRANSFORM
# ----------------------------
def transform_price(ti):
    price = ti.xcom_pull(task_ids="extract_price")

    if price is None:
        raise ValueError("Price is None from API")

    price = float(price)

    if price <= 0:
        raise ValueError(f"Invalid price: {price}")

    transformed = {
        "symbol": "BTC",
        "price_usd": round(price, 2),
        "source": "coingecko",
        "pipeline": "bitcoin_etl"
    }

    logger.info(f"[TRANSFORM] enriched record = {transformed}")

    return transformed


# ----------------------------
# LOAD (RAW layer)
# ----------------------------
def load_price(ti, dag_run):
    data = ti.xcom_pull(task_ids="transform_price")
    run_id = dag_run.run_id

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO warehouse.bitcoin_prices
        (created_at, price_usd, run_id, symbol, source)
        VALUES (NOW(), %s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
    """, (
        data["price_usd"],
        run_id,
        data["symbol"],
        data["source"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"[LOAD] inserted price = {data['price_usd']}")


# ----------------------------
# DEFAULT ARGS
# ----------------------------
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="bitcoin_etl_spark_dag",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    # schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["bitcoin", "etl", "spark"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_price",
        python_callable=extract_price
    )

    transform_task = PythonOperator(
        task_id="transform_price",
        python_callable=transform_price
    )

    load_task = PythonOperator(
        task_id="load_price",
        python_callable=load_price
    )

    # ----------------------------
    # SPARK STEP (NEW)
    # ----------------------------
    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        docker exec spark-master spark-submit \
          --master spark://spark-master:7077 \
          --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
          /opt/bitnami/spark/jobs/btc_aggregation.py
        """
    )

    # ----------------------------
    # PIPELINE ORDER
    # ----------------------------
    extract_task >> transform_task >> load_task >> run_spark_job