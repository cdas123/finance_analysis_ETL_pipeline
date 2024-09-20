from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from fetch import fetch_csv
from transform import transform_and_merge
from save import write_dataframe_to_snowflake
import snowflake.connector
import subprocess



conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE')
    )


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'finance_analysis_ETL',
    default_args=default_args,
    description='A finance anlysis pipeline',
    schedule_interval='@daily',
)


ingestion_task = PythonOperator(
    task_id='ingest_data',
    python_callable=fetch_csv,
    dag=dag,
)

transform_and_merge = transform_and_merge(conn,"savings_csv", "visa_csv",  "buchungstag")

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_merge,
    dag=dag,
)

strore_transformed_data_to_snowflake = write_dataframe_to_snowflake(transform_and_merge, "final")

store_transformed_data = PythonOperator(
    task_id='store_transformed_data',
    python_callable=strore_transformed_data_to_snowflake,
    dag=dag,
)

# Define task dependencies
ingestion_task >> transform >> store_transformed_data