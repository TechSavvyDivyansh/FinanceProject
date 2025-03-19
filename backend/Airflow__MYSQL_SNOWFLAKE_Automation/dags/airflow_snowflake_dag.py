from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime
import logging


MYSQL_CONN_ID="mysql_default"

# DAG definition
with DAG(
    dag_id="mysql_to_snowflake_etl",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "mysql", "snowflake"]
) as dag:

    @task()
    def extract_data():
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        today = datetime.today().strftime('%Y-%m-%d')
        
        logging.info(today)

        sql = f"""
        SELECT * FROM transactions
        WHERE DATE(date) = '{today}';
        """
        df = hook.get_pandas_df(sql)
        return df.to_json(orient="split")

    @task()
    def transform_data(raw_data):
        df = pd.read_json(raw_data, orient="split")
        
        # Example: Add a new column, filter, etc.
        df['processed_at'] = datetime.now()
        df = df.dropna()  # Drop rows with null values
        return df.to_json(orient="split")

    @task()
    def load_data(transformed_data):
        df = pd.read_json(transformed_data, orient="split")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_sqlalchemy_engine()

        # Write to Snowflake (append mode)
        df.to_sql('target_table', con=conn, index=False, if_exists='append')

    # DAG flow
    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)
