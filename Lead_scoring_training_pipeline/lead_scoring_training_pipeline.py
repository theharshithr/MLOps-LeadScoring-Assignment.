##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

from Lead_scoring_training_pipeline.utils import *
from Lead_scoring_training_pipeline.constants import *

###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################
encode_features_task = PythonOperator(
            task_id = 'encoding_categorical_variables',
            python_callable = encode_features,
            op_kwargs={'db_path':DB_PATH,'db_file_name':DB_FILE_NAME, 'ONE_HOT_ENCODED_FEATURES':ONE_HOT_ENCODED_FEATURES, 'FEATURES_TO_ENCODE':FEATURES_TO_ENCODE},
            dag = ML_training_dag)
###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################

get_trained_model_task = PythonOperator(
            task_id = 'training_model',
            python_callable = get_trained_model,
            op_kwargs={'db_path':DB_PATH,'db_file_name':DB_FILE_NAME, 'ONE_HOT_ENCODED_FEATURES':ONE_HOT_ENCODED_FEATURES, 'FEATURES_TO_ENCODE':FEATURES_TO_ENCODE},
            dag = ML_training_dag)
###############################################################################
# Define relations between tasks
# ##############################################################################


encode_features_task >> get_trained_model_task
