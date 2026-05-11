from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'menna',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='ETL_pipeline_sales',
    default_args=default_args,
    description='ETL Pipeline for Global Superstore Sales',
    schedule_interval=None,
    catchup=False,
    tags=['sales', 'etl', 'superstore']
) as dag:

    # Task 1: Extraction (e.py)
    extract = BashOperator(
        task_id='Extracting_data',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/scripts/e.py'
    )

    # Task 2: Transformation (t.py)
    transform = BashOperator(
        task_id='Transformation',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/scripts/t.py'
    )

    # Task 3: Loading (l.py)
    load = BashOperator(
        task_id='Loading',
        bash_command='docker exec spark-jupyter spark-submit /home/jovyan/work/scripts/l.py'
    )

    # Set dependencies (extraction → transformation → loading)
    extract >> transform >> load