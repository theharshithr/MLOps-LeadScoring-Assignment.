##############################################################################
# Import necessary modules
# #############################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

###############################################################################
# Define default arguments and DAG
###############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
###############################################################################
from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task(name='building_db')
def build_dbs(DB_FILE_NAME, DB_PATH):
    db_file = os.path.join(DB_PATH, DB_FILE_NAME)
    if os.path.isfile(db_file):
        print("DB Already Exists")
        return "DB Exists"
    else:
        print("Creating Database")
        conn = sqlite3.connect(db_file)
        print("New DB Created")
        return "DB Created"
###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
###############################################################################
from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task(name='checking_raw_data_schema')
def raw_data_schema_check(data):
    # function body that checks the schema of the raw data
    pass
###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
##############################################################################

###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
###############################################################################

###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
###############################################################################

###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
###############################################################################

###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
###############################################################################

###############################################################################
# Define the relation between the tasks
###############################################################################


