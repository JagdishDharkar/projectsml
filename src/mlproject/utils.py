import os
import sys
from src.mlproject.exception import CustomException
from src.mlproject.logger import logging
import pandas as pd
import pymysql
from dotenv import load_dotenv
import pickle

# Load environment variables from .env file
load_dotenv()

# Get database connection information from environment variables
host = os.getenv("host")
user = os.getenv("user")
password = os.getenv("password")
db = os.getenv('db')

def read_sql_data():
    logging.info("Reading SQL database started")
    try:
        # Establish a connection to the MySQL database
        mydb = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db
        )
        logging.info("Connection Established: %s", mydb)

        # Execute a SQL query to retrieve data from the "students" table
        query = 'SELECT * FROM students'
        df = pd.read_sql_query(query, mydb)

        # Print the first few rows of the DataFrame (for debugging purposes)
        print(df.head())

        return df

    except Exception as ex:
        raise CustomException(ex)

def save_object(file_path, obj):
    try:
        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)

    except Exception as e:
        raise CustomException(e, sys)
