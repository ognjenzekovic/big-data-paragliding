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
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=2
) as dag:

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/scripts/transform.py",
        conn_id="spark_default",
        verbose=True
    )

    batch_q1 = SparkSubmitOperator(
        task_id="batch_q1_categories_monthly",
        application="/opt/airflow/scripts/batch_categories_monthly.py",
        conn_id="spark_default",
    )

    batch_q2 = SparkSubmitOperator(
        task_id="batch_q2_locations_stability",
        application="/opt/airflow/scripts/batch_locations_stability.py",
        conn_id="spark_default",
    )

    batch_q3 = SparkSubmitOperator(
        task_id="batch_q3_hourly",
        application="/opt/airflow/scripts/batch_hourly.py",
        conn_id="spark_default",
    )

    batch_q4 = SparkSubmitOperator(
        task_id="batch_q4_categories_hourly",
        application="/opt/airflow/scripts/batch_categories_hourly.py",
        conn_id="spark_default",
    )

    batch_q5 = SparkSubmitOperator(
        task_id="batch_q5_correlation",
        application="/opt/airflow/scripts/batch_correlation.py",
        conn_id="spark_default",
    )

    batch_q6 = SparkSubmitOperator(
        task_id="batch_q6_dangerous_by_season",
        application="/opt/airflow/scripts/batch_dangerous_by_season.py",
        conn_id="spark_default",
    )

    batch_q7 = SparkSubmitOperator(
        task_id="batch_q7_xc_best_month",
        application="/opt/airflow/scripts/batch_xc_best_month.py",
        conn_id="spark_default",
    )

    batch_q8 = SparkSubmitOperator(
        task_id="batch_q8_afternoon_predition",
        application="/opt/airflow/scripts/batch_prediction.py",
        conn_id="spark_default",
    )

    batch_q9 = SparkSubmitOperator(
        task_id="batch_q9_bhl_development",
        application="/opt/airflow/scripts/batch_bhl_development.py",
        conn_id="spark_default",
    )

    batch_q10 = SparkSubmitOperator(
        task_id="batch_q10_climate_trend",
        application="/opt/airflow/scripts/batch_climate_trend.py",
        conn_id="spark_default",
    )

    transform >> [batch_q1, batch_q2, batch_q3, batch_q4, batch_q5, batch_q6, batch_q7, batch_q8, batch_q9, batch_q10]