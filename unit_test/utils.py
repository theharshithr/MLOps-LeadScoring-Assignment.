##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error

import warnings
warnings.filterwarnings('ignore')

from significant_categorical_level import *
from constants import *
from city_tier_mapping import *

#from Lead_scoring_data_pipeline.mapping.significant_categorical_level import *
#from Lead_scoring_data_pipeline.constants import *
#from Lead_scoring_data_pipeline.mapping.city_tier_mapping import *

###############################################################################
# Define the function to build database
###############################################################################

def build_dbs(db_path, db_file_name):
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 
    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  
    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'
        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'
    SAMPLE USAGE
        build_dbs()
    '''
    if os.path.isfile(db_path+db_file_name):
        print( "DB Already Exists")
        print(os.getcwd())
        return "DB Exists"
    else:
        print ("Creating Database")
        conn = None
        try:
            """ create a database connection to a SQLite database """
            conn = sqlite3.connect(db_path+db_file_name)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"


###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db(db_path, db_file_name, data_directory):
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        
    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.
    SAMPLE USAGE
        load_data_into_db()
    '''
    print("Reading csv file from data directory")
    df_lead_scoring = pd.read_csv(f"{data_directory}{DATA_FILE}")
    
    print("Replacing any null values present in 'toal_leads_dropped' and 'referred_lead' with 0")
    df_lead_scoring['total_leads_droppped'] = df_lead_scoring['total_leads_droppped'].fillna(0)
    df_lead_scoring['referred_lead'] = df_lead_scoring['referred_lead'].fillna(0)
    
    print("Saving the processed dataframe in the db in a table named 'loaded_data")
    cnx = sqlite3.connect(db_path+db_file_name)
    df_lead_scoring.to_sql(name='loaded_data', con=cnx, if_exists='replace',index=False)
    print("Done saving db into loaded db")
    
    cnx.close()


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier(db_path, db_file_name, city_tier_mapping):
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier
    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.
    
    SAMPLE USAGE
        map_city_tier()
    '''
    print("Connecting to database")
    cnx = sqlite3.connect(db_path+db_file_name)
    print("Reading data from loaded_data table")
    loaded_data = pd.read_sql('select * from loaded_data', cnx)
    
    print("Mapping cities to their tier")
    loaded_data["city_tier"] = loaded_data["city_mapped"].map(city_tier_mapping)
    loaded_data["city_tier"] = loaded_data["city_tier"].fillna(3.0)
    
    loaded_data = loaded_data.drop(['city_mapped'], axis = 1)
    
    print("Saving the processed dataframe in the db in a table named 'city_tier_mapped'")
    loaded_data.to_sql(name='city_tier_mapped', con=cnx, if_exists='replace',index=False)
    
    print("Closing database connection")
    cnx.close()


###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################

def map_categorical_vars_for_column(input_df, column_name, column_values):
    # get rows for levels which are not present in column_values
    new_df = input_df[~input_df[column_name].isin(column_values)] 
    # replace the value of these levels to others
    new_df[column_name] = "others" 
    # get rows for levels which are present in column_values
    old_df = input_df[input_df[column_name].isin(column_values)] 
    # concatenate new_df and old_df to get the final dataframe
    df = pd.concat([new_df, old_df])
    return df

def map_categorical_vars(db_path, db_file_name, list_platform, list_medium, list_source):
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source
        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.
    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    print("Connecting to database")
    cnx = sqlite3.connect(db_path+db_file_name)
    print("Reading data from city_tier_mapped table")
    city_tier_mapped = pd.read_sql('select * from city_tier_mapped', cnx)
    
    print("Mapping all the unsignificant variables present in first_platform_c")
    city_tier_mapped = map_categorical_vars_for_column(city_tier_mapped, 'first_platform_c', list_platform)
    print("Mapping all the unsignificant variables present in first_utm_medium_c")
    city_tier_mapped = map_categorical_vars_for_column(city_tier_mapped, 'first_utm_medium_c', list_medium)
    print("Mapping all the unsignificant variables present in first_utm_source_c")
    city_tier_mapped = map_categorical_vars_for_column(city_tier_mapped, 'first_utm_source_c', list_source)
    
    print("Saving the processed dataframe in the db in a table named 'categorical_variables_mapped'")
    city_tier_mapped.to_sql(name='categorical_variables_mapped', con=cnx, if_exists='replace',index=False)
    
    print("Closing database connection")
    cnx.close()



##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping(db_path, db_file_name, interaction_mapping_file, index_columns):
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 
    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.
    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'
    
    SAMPLE USAGE
        interactions_mapping()
    '''
    print("Connecting to database")
    cnx = sqlite3.connect(db_path+db_file_name)
    print("Reading data from categorical_variables_mapped table")
    categorical_variables_mapped = pd.read_sql('select * from categorical_variables_mapped', cnx)
    
    # read the interaction mapping file
    print("Reading interaction_mapping from categorical_variables_mapped table")
    df_event_mapping = pd.read_csv(interaction_mapping_file, index_col=[0])
    
    #check if 'app_complete_flag' is present in 'categorical_variables_mapped' table and 
    #if it is present pass a list with 'app_complete_flag' in it as index_column else 
    #pass a list without 'app_complete_flag' in it.
    if 'app_complete_flag' not in categorical_variables_mapped.columns.tolist():
        index_columns.remove('app_complete_flag')
    
    print("Unpivoting the interaction columns and put the values in rows")
    df_unpivot = pd.melt(categorical_variables_mapped, id_vars=index_columns, 
                         var_name='interaction_type', value_name='interaction_value')
    print("Handling the nulls in the interaction value column")
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    print("Mapping interaction type column with the mapping file to get interaction mapping")
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    print("Dropping the interaction type column as it is not needed")
    df = df.drop(['interaction_type'], axis=1)
    print("Pivoting the interaction mapping column values to individual columns in the dataset")
    df_pivot = df.pivot_table(values='interaction_value', index=index_columns, columns='interaction_mapping', aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    
    print("Saving the processed dataframe in the db in a table named 'interactions_mapped'")
    df_pivot.to_sql(name='interactions_mapped', con=cnx, if_exists='replace',index=False)
    
    print("Closing database connection")
    cnx.close()
