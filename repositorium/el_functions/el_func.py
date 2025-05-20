import os
from datetime import datetime, timedelta
import time
import duckdb
from duckdb import DuckDBPyConnection
import pyarrow as pa
import pyarrow.parquet as pq
from pymssql import Connection
from typing import List, Tuple
from repositorium.logger import logger
from repositorium.el_functions.sql_mapping import MSSQL_TO_DUCKDB_MAP

MAX_RETRIES = 3
RETRY_DELAY = 30

def clear_temp_directory(directory_path: str) -> None:
    """
    Delete all files in the target folder.

    Args:
        directory_path (str): Path to the target folder.
    """
    # Is catalog exists
    if not os.path.isdir(directory_path):
        logger.info(f"The directory '{directory_path}' does not exist.")
        return

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        if os.path.isfile(file_path) and filename != '.gitignore':
            os.remove(file_path)
            logger.info(f"Deleted file: {file_path}")
        else:
            logger.info(f"Skipped: {file_path} (not a file)")
            
    logger.info("All files in the directory have been deleted.")


def fetch_data_to_parquet(
        conn_sql: Connection,
        date_range: dict,
        sql_query: str,
        table_name: str,
        data_dir: str,
        temp_dir: str
        ) -> None:
    """
    Extracts data in chunks from SQL Server, loads it into DuckDB, and exports to Parquet.

    Args:
        conn_sql (Connection): Connection to SQL Server.
        date_range (dict): Dictionary with 'start_date', 'end_date', 'days_per_page'.
        sql_query (str): SQL query with placeholders for date range.
        table_name (str): Name of the target table.
    """
    start_date = datetime.strptime(date_range['start_date'], '%Y-%m-%d').date()
    end_date = datetime.strptime(date_range['end_date'], '%Y-%m-%d').date()
    days_per_page = date_range['days_per_page']

    current_page_start_date = start_date

    temp_db = os.path.join(temp_dir, f"{table_name}.duckdb")

    if os.path.exists(temp_db):
        os.remove(temp_db)  # Remove temp database if it exists
        logger.info(f"Deleted pre-existing temp database: {temp_db}")

    try:
        # Connect to DuckDB
        with duckdb.connect(database=temp_db) as duck_conn:
            # Drop table if exists and recreate
            duck_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            table_created_flag = False

            while current_page_start_date <= end_date:
                current_page_end_date = min(
                    current_page_start_date + timedelta(days=days_per_page - 1), end_date
                    )

                headers, data_types, page_data = fetch_data_chunk(
                    conn_sql=conn_sql,
                    sql_query=sql_query,
                    start_date=current_page_start_date,
                    end_date=current_page_end_date
                )
                

                arrow_table = data_chunk_to_pyarrow(headers=headers, data=page_data)

                if not table_created_flag:

                    create_table_from_cursor_mapping(
                        duck_conn=duck_conn,
                        table_name=table_name,
                        headers=headers,
                        data_types=data_types,
                        sql_dtypes=MSSQL_TO_DUCKDB_MAP
                    )
                    table_created_flag = True
                    
                insert_arrow_to_duckdb(
                    duck_conn=duck_conn,
                    table_name=table_name,
                    arrow_table=arrow_table
                    )
                    
                # set the next chunk date
                current_page_start_date = current_page_end_date + timedelta(days=1)
    
            # Export final table to Parquet
            export_to_parquet(
                duck_conn=duck_conn,
                table_name=table_name,
                output_path=data_dir)

            logger.info(f"Successfully exported {table_name} to {data_dir}.")
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {e}", exc_info=True)
        raise
        
    finally:
        if os.path.isfile(temp_db):
            os.remove(temp_db)
            logger.info(f"Deleted file: {temp_db}")


def fetch_static_data_to_parquet(
        conn_sql: Connection,
        sql_query: str,
        table_name: str,
        data_dir: str,
        temp_dir
        ) -> None:
    """
    Extracts data from SQL Server, loads it into DuckDB, and exports to Parquet.
    
    Args:
        conn_sql (Connection): Connection to SQL Server.
        sql_query (str): SQL query with placeholders for date range.
        table_name (str): Name of the target table.
    """
    temp_db = os.path.join(temp_dir, f"{table_name}.duckdb")

    if os.path.exists(temp_db):
        os.remove(temp_db)  # Remove temp database if it exists
        logger.info(f"Deleted pre-existing temp database: {temp_db}")

    # Connect to DuckDB
    with duckdb.connect(database=temp_db) as duck_conn:
        # Drop table if exists and recreate
        duck_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        headers, data_types, page_data = fetch_data_chunk(
            conn_sql=conn_sql,
            sql_query=sql_query
            )

        create_table_from_cursor_mapping(
            duck_conn=duck_conn,
            table_name=table_name,
            headers=headers,
            data_types=data_types,
            sql_dtypes=MSSQL_TO_DUCKDB_MAP
        )
                
        arrow_table = data_chunk_to_pyarrow(
            headers=headers,
            data=page_data
            )
                
        insert_arrow_to_duckdb(
            duck_conn=duck_conn,
            table_name=table_name,
            arrow_table=arrow_table
            )
        
        # Export final table to Parquet
        export_to_parquet(
            duck_conn=duck_conn,
            table_name=table_name,
            output_path=data_dir)
     
        logger.info(f"Successfully exported {table_name} to {data_dir}.")

        if os.path.isfile(temp_db):
            os.remove(temp_db)
            logger.info(f"Deleted file: {temp_db}")



def fetch_data_chunk(conn_sql: Connection,
                     sql_query:str,
                     time_pause: int = 5,
                     max_retries: int = MAX_RETRIES,
                     retry_delay: int = RETRY_DELAY,
                     start_date: str = None,
                     end_date: str = None,
                     ) -> Tuple[list, list]:
    """
    Fetches a chunk of data from SQL Server, including column names and data types.

    Args:
        conn_sql: Active SQL Server connection.
        sql_query (str): The SQL query with placeholders for the date range.
        start_date (str): Start date for filtering or None for static table.
        end_date (str): End date for filtering or None for static table.

    Returns:
        tuple: (headers, data_types, page_data)
            - headers (list): Column names from the query result.
            - data_types (list): Corresponding SQL Server data types.
            - page_data (list of tuples): Retrieved data rows.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with conn_sql.cursor() as cursor:
                if start_date is None and end_date is None:
                    cursor.execute(sql_query)
                    logger.info(f'Fetching data from compleate')
                else:    
                    cursor.execute(sql_query, (start_date, end_date))
                    logger.info(f'Fetching data from {start_date} to {end_date}')

                # Extract headers and SQL data types
                headers = [col[0] for col in cursor.description]  
                data_types = [col[1] for col in cursor.description]

                logger.info(f'headers: \n {headers} \n \n data_types: \n {data_types }')
                page_data = cursor.fetchall()

                logger.info(f'\n \n Start pause {time_pause} sek. \n')
                time.sleep(time_pause)

            return headers, data_types, page_data

        except Exception as e:
            logger.error(f"Error fetching data chunk: {e}")
            if attempt == max_retries:
                logger.error("All retry attempts failed.", exc_info=True)
                raise  # re-raise the last exception
            else:
                logger.warning(f"Retrying after {retry_delay}s (attempt {attempt}/{max_retries})...")
                time.sleep(retry_delay)
                retry_delay = retry_delay * 2
            


def data_chunk_to_pyarrow(headers: list, data: list
                                ) -> pa.Table:
    """
    Saves a chunk of data to a PyArrow Table.

    Args:
        headers (list): Column names.
        data (list): Data rows.
    """
    # Convert data to PyArrow Table
    data_dict = {headers[i]: [row[i] for row in data] for i in range(len(headers))}
    arrow_table = pa.table(data_dict)
    return arrow_table


def create_table_from_cursor_mapping(
        duck_conn: DuckDBPyConnection,
        table_name: str,
        headers: list,
        data_types: list,
        sql_dtypes: dict[int, str]) -> None:
    """
    Creates a final DuckDB table with correct data types based on SQL Server schema mapping.

    Args:
        duck_conn (DuckDBPyConnection): DuckDB connection object.
        table_name (str): Name of the table.
        headers (list): List of column names.
        data_types (list): Corresponding SQL Server data types.
        sql_dtypes (dict[int, str]): Mapping of SQL Server types to DuckDB types.
    """

    # Build column definitions using SQL Server → DuckDB type mapping
    columns_def = []
    for col, dtype in zip(headers, data_types):
        duckdb_type = sql_dtypes.get(
            dtype, "VARCHAR")  # Default to VARCHAR
        safe_col = f'"{col}"'  # Ensure column names are properly quoted
        columns_def.append(f"{safe_col} {duckdb_type}")

    # Construct the CREATE TABLE statement
    create_table_query = f"""
        CREATE OR REPLACE TABLE "{table_name}" (
            {', '.join(columns_def)}
        );
    """
    
    # Print the final SQL statement for debugging
    logger.info(f"\n Executing CREATE TABLE statement:\n{create_table_query}\n")

    # Execute the query in DuckDB
    duck_conn.execute(create_table_query)


def insert_arrow_to_duckdb(duck_conn: DuckDBPyConnection,
                         table_name: str,
                         arrow_table: pa.Table) -> None:
    """
    Efficiently loads a PyArrow Table directly into a DuckDB table.

    Args:
        duck_conn (duckdb.DuckDBPyConnection): Active DuckDB connection.
        table_name (str): The name of the target DuckDB table.
        arrow_table (pa.Table): PyArrow Table containing the data to load.
    """
    # Register the PyArrow Table as a DuckDB table
    duck_conn.register("arrow_temp_table", arrow_table)

    # Copy data from the Arrow Table to DuckDB
    duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_temp_table")

    logger.info(f"Successfully loaded data into '{table_name}' from PyArrow Table.")


def export_to_parquet(
        duck_conn: DuckDBPyConnection,
        table_name: str,
        output_path: str) -> None:
    """
    Exports a DuckDB table to a Parquet file.

    Args:
        duck_conn (DuckDBPyConnection): DuckDB connection object.
        table_name (str): Name of the table to be exported.
        output_path (str): Path where the Parquet file will be stored.
    """
    duck_conn.execute(
        f"""--sql
        COPY {table_name} 
        TO '{output_path}/{table_name}.parquet' 
        (FORMAT PARQUET);
        """
        )
    logger.info(f"Table {table_name} exported to {output_path}")
