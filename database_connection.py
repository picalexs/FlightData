import os
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

load_dotenv()

try:
    import oracledb
    ORACLEDB_AVAILABLE = True
except ImportError:
    ORACLEDB_AVAILABLE = False

class DatabaseConnection:
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.engine = None
        self.connection_type = None
        
    def get_oracle_engine(self, data_dir: str = "data"):
        try:
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD') 
            db_connection_string = os.getenv('DB_CONNECTION_STRING')
            if db_user:
                db_user = db_user.strip('"\'')
            if db_password:
                db_password = db_password.strip('"\'')
            if db_connection_string:
                db_connection_string = db_connection_string.strip('"\'')
            if not all([db_user, db_password, db_connection_string]):
                raise Exception("Missing Oracle connection details in .env file")
            self.logger.info("Using Oracle database from .env configuration")
            if not ORACLEDB_AVAILABLE:
                raise Exception("oracledb library not available - install with: pip install oracledb")
            self.logger.info(f"Connecting to Oracle at {db_connection_string} as {db_user}")
            test_conn = oracledb.connect(
                user=db_user,
                password=db_password,
                dsn=db_connection_string
            )
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            test_conn.close()
            self.logger.info("[OK] Direct Oracle connection successful")
            encoded_password = quote_plus(db_password)
            connection_url = f"oracle+oracledb://{db_user}:{encoded_password}@{db_connection_string}"
            engine = create_engine(connection_url, echo=False)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            self.logger.info(f"[OK] Oracle SQLAlchemy engine created successfully")
            self.logger.info(f"[OK] Connected to: {db_connection_string}")
            self.engine = engine
            self.connection_type = "oracle"
            return engine
        except Exception as e:
            self.logger.error(f"Oracle connection failed: {e}")
            return None
    
    def get_sqlite_fallback(self, data_dir: str = "data") -> Any:
        try:
            self.logger.info("Creating SQLite fallback database...")
            os.makedirs(data_dir, exist_ok=True)
            sqlite_path = os.path.join(data_dir, "flight_data.db")
            engine = create_engine(f"sqlite:///{sqlite_path}")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(f"[OK] SQLite database ready: {sqlite_path}")
            self.engine = engine
            self.connection_type = "sqlite"
            return engine
        except Exception as e:
            self.logger.error(f"SQLite fallback failed: {e}")
            return None
    
    def get_database_engine(self, data_dir: str = "data", prefer_oracle: bool = True):
        
        if prefer_oracle:
            oracle_engine = self.get_oracle_engine(data_dir)
            if oracle_engine:
                return oracle_engine
                
        self.logger.warning("Falling back to SQLite database")
        return self.get_sqlite_fallback(data_dir)
    
    def get_connection_info(self) -> Dict[str, Any]:
        return {
            'type': self.connection_type,
            'engine': self.engine,
            'is_oracle': self.connection_type == 'oracle',
            'is_sqlite': self.connection_type == 'sqlite'
        }

def create_database_connection(logger: Optional[logging.Logger] = None, data_dir: str = "data"):
    db_conn = DatabaseConnection(logger)
    engine = db_conn.get_database_engine(data_dir)
    return engine, db_conn.get_connection_info()
