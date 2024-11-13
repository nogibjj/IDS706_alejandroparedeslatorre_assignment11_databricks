#!pip install databricks-sql-connector
#!pip install databricks
#!pip install python-dotenv
#%restart_python

from lib.extract_transform_load import ETL
from lib.query_db import spark_sql_query
#from data.db_connection import DBConnection

#import subprocess
#import sys

#def install(name):
    #subprocess.call([sys.executable, '-m', 'pip', 'install', name])

def single_query_main():
    spark_sql_query("SELECT * FROM aplt_universities") #WHERE state_province IS NOT NULL

if __name__ == "__main__":
    #install('databricks-sql-connector')
    #install('databricks')
    #install('python-dotenv')
    single_query_main()

