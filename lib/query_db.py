import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws
from pyspark.sql.types import StringType
#from data.db_connection import DBConnection

# Initialize the Spark session (optional in Databricks, as Spark is often pre-configured)
spark = SparkSession.builder.appName("DatabricksDirectQuery").getOrCreate()

def spark_sql_query(query):
    # Execute the SQL query directly using Spark SQL
    result_df = spark.sql(query)
    
    # Convert the result to Pandas for easy display
    output = result_df.limit(10).toPandas().to_markdown()
    print("Spark SQL query executed successfully:\n", output)

if __name__ == "__main__":
  print('started process')
  spark_sql_query('select * from aplt_universities')