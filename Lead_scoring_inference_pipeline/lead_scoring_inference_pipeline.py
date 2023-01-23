##############################################################################
# Import necessary modules
# #############################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from Lead_scoring_inference_pipeline.utils import *
from Lead_scoring_inference_pipeline.constants import *

###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@hourly',
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################
encode_features_task = PythonOperator(
            task_id = 'encoding_categorical_variables',
            python_callable = encode_features,
            op_kwargs={'db_path': DB_PATH,'db_file_name':DB_FILE_NAME, 'ONE_HOT_ENCODED_FEATURES':ONE_HOT_ENCODED_FEATURES,  'FEATURES_TO_ENCODE':FEATURES_TO_ENCODE},
            dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
input_features_check_task = PythonOperator(
            task_id = 'checking_input_features',
            python_callable = input_features_check,
            op_kwargs={'db_path': DB_PATH,'db_file_name':DB_FILE_NAME, 'ONE_HOT_ENCODED_FEATURES':ONE_HOT_ENCODED_FEATURES},
            dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
get_models_prediction_task = PythonOperator(
            task_id = 'generating_models_prediction',
            python_callable = get_models_prediction,
            op_kwargs={'db_path': DB_PATH,'db_file_name':DB_FILE_NAME, 'MODEL_NAME':MODEL_NAME, 'STAGE':STAGE},
            dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
prediction_ratio_check_task = PythonOperator(
            task_id = 'checking_model_prediction_ratio',
            python_callable = prediction_ratio_check,
            op_kwargs={'db_path': DB_PATH,'db_file_name':DB_FILE_NAME},
            dag = Lead_scoring_inference_dag)


###############################################################################
# Define relation between tasks
# ##############################################################################
encode_features_task >> input_features_check_task >> get_models_prediction_task >> prediction_ratio_check_task
