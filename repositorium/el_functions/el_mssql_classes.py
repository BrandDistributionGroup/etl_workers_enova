
import logging
from datetime import datetime
import pymssql
from pymssql import Connection
from repositorium.el_functions import el_func

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MssqlConnection:
    """Class for managing connection to MS SQL Server."""
    
    def __init__(self,
                 server: str,
                 username: str,
                 password: str,
                 database: str,
                 db_timeout_limit: int=300):
        self.server = server
        self.username = username
        self.password = password
        self.database = database
        self.time_out_limit = db_timeout_limit
        self.conn_sql = None  # Connection will be established on the first request

    def __enter__(self):
        """Establishes a connection to the database when entering the context manager."""
        try:
            self.conn_sql = pymssql.connect(
                server=self.server,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=self.time_out_limit
            )
            logger.info("Database connection established successfully.")
            return self.conn_sql
        except Exception as e:
            logger.error(f"Failed to establish database connection: {e}")
            raise
        

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures that the connection is closed upon exit."""
        if self.conn_sql:
            self.conn_sql.close()
            logger.info("Database connection closed.")


class DatabaseTablesExtractor:
    """Class for processing static tables and saving them in Parquet format."""
    
    def __init__(self,
                 data_dir: str,
                 temp_dir: str,
                 date_range: dict=None
                 ):
        self.data_dir = data_dir
        self.temp_dir = temp_dir
        self.date_range = date_range
        self.conn_sql = None

    def set_connection(self, conn_sql: pymssql.Connection):
        """Sets the database connection."""
        self.conn_sql = conn_sql

    def create_parquet(self, table_query: str, table_name: str) -> None:
        """Creates a Parquet file from an SQL query."""
        if self.conn_sql is None:
            logger.error(ValueError("Database connection is not established. Use set_connection()."))
            raise

        logger.info(f"Creating Parquet file for table: {table_name}")
        el_func.fetch_static_data_to_parquet(
            conn_sql=self.conn_sql,
            sql_query=table_query,
            table_name=table_name,
            data_dir=self.data_dir,
            temp_dir=self.temp_dir
        )
        logger.info(f"Parquet file created successfully for table: {table_name}")

    def create_parquet_date_depended(self, table_query: str, table_name: str) -> None:
        """Creates a Parquet file from an SQL query."""
        if self.conn_sql is None:
            logger.error(ValueError("Database connection is not established. Use set_connection()."))
            raise

        logger.info(f"Creating Parquet file for table: {table_name}")
        el_func.fetch_data_to_parquet(
            conn_sql=self.conn_sql,
            date_range=self.date_range,
            sql_query=table_query,
            table_name=table_name,
            data_dir=self.data_dir,
            temp_dir=self.temp_dir
        )
        logger.info(f"Parquet file created successfully for table: {table_name}")

