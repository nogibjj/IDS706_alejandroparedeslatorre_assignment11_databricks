from lib.extract_transform_load import ETL

def main():
    e_t_l = ETL()
    e_t_l.extract()
    e_t_l.transform()
    #e_t_l.load()
    e_t_l.spark_sql_query("SELECT name, country, state_province FROM universities WHERE state_province IS NOT NULL")

if __name__ == "__main__":
    main()
