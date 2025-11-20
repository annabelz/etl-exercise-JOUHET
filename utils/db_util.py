import psycopg2
from data_pipeline.utils import logger_util

logger = logger_util.get_logger(__name__)

def get_connection(
  host: str = "localhost",
  port: int = 5432,
  dbname: str = "phds_db",
  user: str = "phds_admin",
  password: str = "example"
):
    try: 
        logger.info(f"Connecting to the database {dbname}")
        conn = psycopg2.connect(
            host = host,
            port = port,
            dbname = dbname,
            user = user,
            password = password
        )
        logger.info(f"Connected to the database {dbname}")
        return conn
    except Exception as e: 
        logger.error(f"Error while connecting to the database {dbname}: {e}")
        raise 
    