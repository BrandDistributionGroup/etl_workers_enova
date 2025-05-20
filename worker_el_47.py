import os
import sys
import duckdb
from repositorium.config_modul import config
from repositorium.logger import logger
from data_source.sql_tables_data import (
    COMPANY_DATABASE_LIST,
    ACCOUNTS_47_QUERY)
from repositorium.el_functions.el_mssql_classes import (
    MssqlConnection,
    DatabaseTablesExtractor)

# Source db
server = config.get('enova_finance_database', 'server')
username = config.get('enova_finance_database', 'username')
password = config.get('enova_finance_database', 'password')

# Target db
dbname = config.get('dw_database', 'db_name')
dbhost = config.get('dw_database', 'db_host')
dbuser = config.get('dw_database', 'db_user')
dbpassword = config.get('dw_database', 'db_password')

data_dir = './parquet2load'
temp_dir = './temp'

def main():

    # Extract data
    for company_name, table_name, company_db in COMPANY_DATABASE_LIST:

        logger.info(f'{company_name} {table_name} {company_db}')

        with MssqlConnection(server=server,
                            username=username,
                            password=password,
                            database=company_db) as conn:
            
            extractor = DatabaseTablesExtractor(data_dir=data_dir,
                                                temp_dir=temp_dir)
            
            
            extractor.set_connection(conn_sql=conn)
            extractor.create_parquet(
                table_query=ACCOUNTS_47_QUERY.format(company_name=company_db),
                table_name=table_name
            )

    # Load data
    ATTACH_STRING = f'''ATTACH 'dbname={dbname}
            user={dbuser}
            password={dbpassword}
            host={dbhost}'
            AS st_db (TYPE POSTGRES, SCHEMA 'public');'''
              
  
    try:
        with duckdb.connect(':memory:') as conn:
            conn.execute(ATTACH_STRING)
            logger.info(f'The db {dbname} has been attached')

            for file_name in os.listdir(data_dir):
                if file_name.endswith('.parquet'):
                    table_name = os.path.splitext(file_name)[0]
                    file_path = os.path.join(data_dir, file_name)

                    # Drop table if it exists
                    conn.execute(f"""
                        DROP TABLE IF EXISTS st_db.{table_name} CASCADE;
                    """)

                    # Create empty table based on schema
                    conn.execute(f"""
                        CREATE TABLE st_db.{table_name} AS 
                        SELECT * FROM read_parquet('{file_path}') WHERE 1=0;
                    """)

                    # Load data into the table
                    conn.execute(f"""
                        COPY st_db.{table_name} FROM '{file_path}' (FORMAT PARQUET);
                    """)

                    logger.info(f'The file {file_name} has been successfully loaded to the database {dbname}')

    except Exception as e:
        logger.error(f'Error occurred while loading parquet files to database {dbname}: {e}', exc_info=False)
        raise

    logger.info("Task finished successfully, exiting now")    
    return 0            
        
if __name__ == '__main__':
    exit_code = main()
    logger.info(f"Exiting with code {exit_code}")
    sys.exit(exit_code)