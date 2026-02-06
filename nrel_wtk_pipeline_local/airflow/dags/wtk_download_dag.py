
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import mysql.connector

# --- Task Functions ---
def ingest_data():
    api_key = os.getenv('NREL_API_KEY')
    print(f"Fetching data using API Key: {api_key}")
    # Example API call (replace with actual endpoint)
    url = f"https://developer.nrel.gov/api/wind-toolkit/v2/wind/wtk-download.json?api_key={api_key}"
    response = requests.get(url)
    print(f"Response Status: {response.status_code}")
    # Save raw data
    with open('/tmp/wtk_raw.json', 'w') as f:
        f.write(response.text)

def transform_data():
    print("Transforming data...")
    # Add transformation logic here
    # For example: parse JSON, clean data, prepare for DB insert

def load_to_mysql():
    print("Loading data into MySQL...")
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DB')
    )
    cursor = conn.cursor()
    # Example insert (replace with actual schema)
    cursor.execute("CREATE TABLE IF NOT EXISTS wtk_data (id INT AUTO_INCREMENT PRIMARY KEY, data TEXT)")
    cursor.execute("INSERT INTO wtk_data (data) VALUES ('Sample Data')")
    conn.commit()
    cursor.close()
    conn.close()

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='wtk_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql
    )

    ingest_task >> transform_task >> load_task
