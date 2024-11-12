import requests
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws
from pyspark.sql.types import StringType
import pandas as pd

class ETL:
    """Extracts data from an API, transforms it using PySpark, and loads it into a SQLite database."""

    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("ETL University Data").getOrCreate()
        self.df_spark = None

    def log_output(self, operation, output, query=None):
        LOG_FILE = "pyspark_output.md"
        """Adds log information to a markdown file."""
        with open(LOG_FILE, "a") as file:
            file.write(f"## {operation}\n\n")
            if query: 
                file.write(f"### Query:\n```\n{query}\n```\n\n")
            file.write("### Output:\n\n")
            file.write(f"```\n{output}\n```\n\n")

    def extract(self, url="http://universities.hipolabs.com/search?country=United+States"):
        self.log_output("Initiating Extract", "Fetching data from API.")
        # Request data from API
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Load data into a Spark DataFrame
            self.df_spark = self.spark.createDataFrame(data)
            output = self.df_spark.limit(10).toPandas().to_markdown()  # First 10 rows as markdown table
            self.log_output("Data Extracted", output)
            print("Data extracted successfully.")
        else:
            error_message = f"Failed to fetch data. Status code: {response.status_code}"
            self.log_output("Extract Failed", error_message)
            print(error_message)

    def transform(self):
        self.log_output("Initiating Transform", "Transforming data...")
        # Explode 'domains' and 'web_pages' lists to create one row per domain and web page
        self.df_spark = self.df_spark \
            .withColumn("domains", explode("domains")) \
            .withColumn("web_pages", explode("web_pages"))

        # Concatenate 'name' and 'country' for a unique identifier and clean column names
        self.df_spark = self.df_spark \
            .withColumn("unique_id", concat_ws("_", col("name"), col("country"))) \
            .withColumnRenamed("alpha_two_code", "country_code") \
            .withColumn("state_province", col("state-province").cast(StringType())) \
            .select("unique_id", "name", "country", "country_code", "state_province", "domains", "web_pages")

        output = self.df_spark.limit(10).toPandas().to_markdown()
        self.log_output("Data Transformed", output)
        print("Data transformed successfully.")

    def load(self):
        self.log_output("Initiating Load", "Loading data into SQLite database.")
        # Convert Spark DataFrame to Pandas for SQLite insertion
        df_pandas = self.df_spark.toPandas()

        # Connect to SQLite database and create table if not exists
        conn = sqlite3.connect("./data/UniversitiesDB.db")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Universities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                unique_id TEXT,
                name TEXT,
                country TEXT,
                country_code TEXT,
                state_province TEXT,
                domains TEXT,
                web_pages TEXT
            )
        """)

        # Insert data into SQLite
        for _, row in df_pandas.iterrows():
            conn.execute("""
                INSERT INTO Universities (unique_id, name, country, country_code, state_province, domains, web_pages)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row['unique_id'], row['name'], row['country'], row['country_code'], row['state_province'], row['domains'], row['web_pages']))

        conn.commit()
        conn.close()
        self.log_output("Data Loaded", "Data successfully loaded into UniversitiesDB.db.")
        print("Data loaded into UniversitiesDB.db")

        # Verify the data
        conn = sqlite3.connect("./data/UniversitiesDB.db")
        query = "SELECT * FROM Universities LIMIT 5"
        result = pd.read_sql_query(query, conn)
        self.log_output("Data Verification", result.to_markdown())
        conn.close()

    def spark_sql_query(self, query):
        self.log_output("Initiating SQL Query", "Executing SQL query on Spark DataFrame.")
        # Register the DataFrame as a temporary SQL view
        self.df_spark.createOrReplaceTempView("universities")

        # Execute the SQL query and log the results
        result_df = self.spark.sql(query)
        output = result_df.limit(10).toPandas().to_markdown()
        self.log_output("SQL Query Executed", output, query=query)
        print("Spark SQL query executed successfully.")

if __name__ == "__main__":
    e_t_l = ETL()
    e_t_l.extract()
    e_t_l.transform()
    e_t_l.load()
    e_t_l.spark_sql_query("SELECT name, country, state_province FROM universities WHERE state_province IS NOT NULL")
