import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Google Cloud Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "london-transport-453517")
DATASET_ID = os.getenv("DATASET_ID", "LONDON_DATA")
TABLE_ID = os.getenv("TABLE_ID", "TFL_JOURNEYS_TYPE")

# Google Cloud Authentication
SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "london-transport-453517-5a7bb63f5525.json")

# CSV File Path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "tfl-journeys-type.csv")

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("BigQueryPipeline")
