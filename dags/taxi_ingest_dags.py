import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_taxi_data():
    conn_str = 'postgresql://admin:password123@dw_postgres:5432/nyc_warehouse'
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    
    try:
        logger.info("Establishing connection to Postgres warehouse...")
        engine = create_engine(conn_str)
        
        logger.info(f"Downloading data from {url}...")
        df = pd.read_parquet(url)
        
        row_count = len(df)
        logger.info(f"Successfully downloaded {row_count} rows. Starting load...")
        
        df.head(100000).to_sql(
            name='raw_nyc_trips', 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        logger.info("Successfully loaded 100,000 rows to table 'raw_nyc_trips'.")
        
    except Exception as e:
        logger.error(f"Ingestion failed. Error details: {str(e)}")
        raise e

with DAG(
    dag_id='nyc_taxi_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@monthly',
    catchup=False
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_taxi_data
    )

    task_dbt = BashOperator(
        task_id='run_dbt_transforms',
        bash_command=(
            'cp -r /opt/airflow/dbt_project /tmp/dbt_project && '
            'cd /tmp/dbt_project && '
            'dbt build --profiles-dir /opt/airflow/dbt_profiles'
        ),
    )

    task_ingest >> task_dbt
