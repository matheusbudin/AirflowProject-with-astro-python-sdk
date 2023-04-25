import astro
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 25), #define a date to start the ingestion job
    'email': ['putYourEmailHere@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG object
dag = DAG(
    'my_api_task',
    default_args=default_args,
    description='A DAG to fetch data from an API and store it in a database',
    schedule_interval='0 0 * * *', # run the task every day at midnight
)

# define the Python function that will be used as the task
def fetch_and_store_data():
    # make the API request using Astro Python SDK
    api_data = astro.requests.get('https://api.example.com/data') #overwrite with a API URL of your choice
    
    # store the data in the database using Astro Python SDK
    db_conn = astro.connect('postgresql://username:password@host:port/database') # Choose a DB as well
    cursor = db_conn.cursor()
    cursor.execute('INSERT INTO my_table (data) VALUES (%s)', (api_data,))
    db_conn.commit()
    db_conn.close()

# define the prepare_data task
def prepare_data():
    # do some data preparation work it can be a simples data cleaning
    pass

# define the operator for prepare_data
run_prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag,
)


# define the operator that will run the task
run_task = PythonOperator(
    task_id='fetch_and_store_data',
    python_callable=fetch_and_store_data,
    dag=dag,
)

# set the dependencies of the task
run_prepare_data >> run_task
