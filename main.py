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

def main():
    #db_conn = DBConnection()
    #db_conn.connect()

    #etl = ETL(db_conn)
    etl = ETL()
    etl.extract()
    etl.transform()
    etl.load("aplt_universities")
    etl.spark_sql_query("SELECT name, country, state_province FROM aplt_universities WHERE state_province IS NOT NULL")


    #db_conn.close()
    spark_sql_query("SELECT * FROM aplt_universities") #WHERE state_province IS NOT NULL

if __name__ == "__main__":
    main()

