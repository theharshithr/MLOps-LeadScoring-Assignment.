"""
Import necessary modules
############################################################################## 
"""
import pandas as pd
import os
import sqlite3
from sqlite3 import Error

###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check(db_path, db_file_name, raw_data_schema):
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    print("Connecting to database")
    cnx = sqlite3.connect(db_path+db_file_name)
    print("Reading data from loaded_data table")
    loaded_data = pd.read_sql('select * from loaded_data', cnx)
    loaded_data_columns = list(loaded_data.columns)
    print("loaded_data column length: ", len(loaded_data_columns))
    print("raw data schema",raw_data_schema)
    print("raw_data_schema length: ", len(raw_data_schema))
    
    column_mismatch = 0
    for col in raw_data_schema:
        if col not in loaded_data_columns:
            print("column mismatch: ", col)
            column_mismatch = 1
            
    if column_mismatch == 0:
        print("Raw datas schema is in line with the schema present in schema.py")
    else:
        print("Raw datas schema is NOT in line with the schema present in schema.py")
    
    print("Closing database connection")
    cnx.close()

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check(db_path, db_file_name, raw_data_schema):
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    

    print("Connecting to database")
    cnx = sqlite3.connect(db_path+db_file_name)
    print("Reading data from interactions_mapped table")
    interactions_mapped = pd.read_sql('select * from interactions_mapped', cnx)

    interactions_mapped_columns = list(interactions_mapped.columns)
    print("interactions_mapped column length: ", len(interactions_mapped_columns))
    print("raw_data_schema", raw_data_schema)
    print("raw_data_schema length: ", len(raw_data_schema))
    
    column_mismatch = 0
    for col in raw_data_schema:
        if col not in interactions_mapped_columns:
            print("column mismatch: ", col)
            column_mismatch = 1
            
    if column_mismatch == 0:
        print("Models input schema is in line with the schema present in schema.py")
    else:
        print("Models input schema is NOT in line with the schema present in schema.py")
    
    print("Closing database connection")
    cnx.close()


    
    
    
