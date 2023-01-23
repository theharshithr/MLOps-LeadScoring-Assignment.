DB_PATH = '/home/airflow/dags/Lead_scoring_data_pipeline/'
DB_FILE_NAME = "lead_scoring_data_cleaning.db"
#DB_FILE_NAME = "lead_scoring_data_cleaning_inference.db"

DB_FILE_MLFLOW_PATH = '/home/airflow/dags/Lead_scoring_training_pipeline/'
DB_FILE_MLFLOW = "Lead_scoring_mlflow_production.db"

FILE_PATH = '/home/airflow/dags/Lead_scoring_inference_pipeline/prediction_distribution.txt'

TRACKING_URI = 'http://0.0.0.0:6006'

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = 'LightGBM'
STAGE = 'Production'
EXPERIMENT = "Lead_scoring_mlflow_production"

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['city_tier_1.0', 'city_tier_2.0', 'city_tier_3.0', 'first_platform_c_Level0', 'first_platform_c_Level1', 'first_platform_c_Level2', 'first_platform_c_Level3', 'first_platform_c_Level7', 'first_platform_c_Level8', 'first_platform_c_others', 'first_utm_medium_c_Level0', 'first_utm_medium_c_Level10', 'first_utm_medium_c_Level11', 'first_utm_medium_c_Level13', 'first_utm_medium_c_Level15', 'first_utm_medium_c_Level16', 'first_utm_medium_c_Level2', 'first_utm_medium_c_Level20', 'first_utm_medium_c_Level26', 'first_utm_medium_c_Level3', 'first_utm_medium_c_Level30', 'first_utm_medium_c_Level33', 'first_utm_medium_c_Level4', 'first_utm_medium_c_Level43', 'first_utm_medium_c_Level5', 'first_utm_medium_c_Level6', 'first_utm_medium_c_Level8', 'first_utm_medium_c_Level9', 'first_utm_medium_c_others', 'first_utm_source_c_Level0', 'first_utm_source_c_Level14', 'first_utm_source_c_Level16', 'first_utm_source_c_Level2', 'first_utm_source_c_Level4', 'first_utm_source_c_Level5', 'first_utm_source_c_Level6', 'first_utm_source_c_Level7', 'first_utm_source_c_others', 'total_leads_droppped', 'referred_lead']
# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['city_tier', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']