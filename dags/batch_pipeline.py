from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "paragliding",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Paragliding batch obrada ERA5 podataka",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/scripts/transform.py",
        conn_id="spark_default",
        verbose=True
    )

    batch_q1 = SparkSubmitOperator(
        task_id="batch_q1_flying_hours",
        application="/opt/airflow/scripts/batch_q1.py",
        conn_id="spark_default",
    )

    batch_q2 = SparkSubmitOperator(
        task_id="batch_q2_stability",
        application="/opt/airflow/scripts/batch_q2.py",
        conn_id="spark_default",
    )

    batch_q3 = SparkSubmitOperator(
        task_id="batch_q3_hourly",
        application="/opt/airflow/scripts/batch_q3.py",
        conn_id="spark_default",
    )

    transform >> [batch_q1, batch_q2, batch_q3]