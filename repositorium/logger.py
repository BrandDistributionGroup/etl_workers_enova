import logging
import os

# Configurable log level
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()

# Set log format
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)d - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Create logger
logger = logging.getLogger("ETL_Logger")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))  # Default DEBUG

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler
log_dir = "/app/logs" if os.getenv('ENV') == 'production' else "./logs"
os.makedirs(log_dir, exist_ok=True)

file_handler = logging.FileHandler(f"{log_dir}/etl.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(console_formatter)
logger.addHandler(file_handler)

def log_exception(exc):
    """Logs exceptions with traceback"""
    logger.error("Exception occurred", exc_info=True)



## Example 
## in main()

# from logger import logger, log_exception

# def main():
#     logger.info("ETL process started")
#     try:
#         # Your ETL logic
#         logger.info("ETL process completed successfully")
#     except Exception as e:
#         log_exception(e)
#         raise

# if __name__ == "__main__":
#     main()


## in function

# from logger import logger

# def extract_data():
#     logger.info("Starting data extraction")
#     try:
#         # Your extraction logic
#         logger.debug("Fetching data from DB")
#         logger.info("Data extraction completed")
#     except Exception as e:
#         logger.error("Error in data extraction", exc_info=True)