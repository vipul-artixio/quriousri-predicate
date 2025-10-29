import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration class for USA FDA Drug fetcher"""
    FDA_BULK_DOWNLOAD_URL = "https://download.open.fda.gov/drug/drugsfda/drug-drugsfda-0001-of-0001.json.zip"
    FDA_API_BASE_URL = "https://api.fda.gov/drug/drugsfda.json"
    BATCH_SIZE = 1000 
    MAX_RETRIES = 3
    RETRY_DELAY = 2  
    REQUEST_TIMEOUT = 300 
    FDA_MAX_SKIP = 25000 
    TRIAL_LIMIT = 0  
    DB_HOST = os.getenv('PG_HOST', 'localhost')
    DB_PORT = os.getenv('PG_PORT', '5432')
    DB_NAME = os.getenv('PG_DATABASE', 'quriousri_db')
    DB_USER = os.getenv('PG_USER', 'postgres')
    DB_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')

    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
    RAW_DATA_FILE = os.path.join(OUTPUT_DIR, 'fda_drugs_raw.json')
    PROCESSED_DATA_FILE = os.path.join(OUTPUT_DIR, 'fda_drugs_processed.json')
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_database_url(cls):
        """Returns the database connection URL"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def ensure_output_dir(cls):
        """Ensures the output directory exists"""
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)

