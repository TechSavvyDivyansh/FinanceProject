from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime
import logging
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


MYSQL_CONN_ID="mysql_default"
SNOWFLAKE_CONN_ID="snowflake_default"
WAREHOUSE_NAME = "FINANCEWAREHOUSE" 

# DAG definition
with DAG(
    dag_id="mysql_to_snowflake_etl",
    start_date=days_ago(1),
    schedule_interval="25 15 * * *",  # ⏰ 9:00 PM daily in ist but uts-> 15:25 
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
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Set context: warehouse, database, schema
            cursor.execute(f"USE WAREHOUSE FINANCEWAREHOUSE")
            cursor.execute("USE DATABASE FINANCE_DATABASE_SNOWFLAKE")
            cursor.execute("USE SCHEMA TRANSACTIONS")

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="transaction_statement",
                auto_create_table=True
            )

            if success:
                logging.info(f"✅ Successfully loaded {nrows} rows to Snowflake.")
            else:
                logging.error("❌ Failed to load data into Snowflake.")
                raise Exception("Failed to load data into Snowflake")

        finally:
            cursor.close()
            conn.close()
    # DAG flow
    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)
