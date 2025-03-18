from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import snowflake.connector
from datetime import datetime, timedelta
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define MySQL and Snowflake connections
MYSQL_CONN = "mysql+pymysql://root:111@localhost:3306/finance_megaProject_db"
SNOWFLAKE_CONN = {
    "user": "TechSavvyDivyansh",
    "password": "DivyanshModi@2510",
    "account": "kdvkmnh-ovb48419",
    # "warehouse": "your_warehouse",
    "database": "FINANCE_DATABASE_SNOWFLAKE",
    "schema": "TRANSACTIONS"
}

# Define MySQL → Snowflake transfer function
def transfer_data():
    # Connect to MySQL
    mysql_engine = create_engine(MYSQL_CONN)
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Check if today's data exists
    query = f"SELECT * FROM your_table WHERE date(created_at) = '{today}'"
    df = pd.read_sql(query, mysql_engine)

    if df.empty:
        print("No new data for today. Skipping transfer.")
        return
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cursor = conn.cursor()

    # Insert data into Snowflake
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO your_table (col1, col2, col3) VALUES 
        ('{row['col1']}', '{row['col2']}', '{row['col3']}')
        """
        cursor.execute(insert_query)

    cursor.close()
    conn.close()
    print("Data transfer completed.")

# Define MySQL cleanup function
def cleanup_mysql():
    # Connect to MySQL
    mysql_engine = create_engine(MYSQL_CONN)
    
    # Delete records older than 1 year
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    delete_query = f"DELETE FROM your_table WHERE date(created_at) < '{one_year_ago}'"
    
    with mysql_engine.connect() as conn:
        conn.execute(delete_query)
        conn.commit()
    
    print("Old data cleanup completed.")

# Define DAG
dag = DAG(
    'mysql_to_snowflake',
    default_args=default_args,
    schedule_interval='35 17 * * *',  # Runs every day at 5 PM (UTC)
    catchup=False
)

# Define tasks
transfer_task = PythonOperator(
    task_id='transfer_mysql_to_snowflake',
    python_callable=transfer_data,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_mysql_data',
    python_callable=cleanup_mysql,
    dag=dag
)

# Task dependencies
transfer_task >> cleanup_task
