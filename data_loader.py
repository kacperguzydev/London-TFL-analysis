from bigquery_handler import BigQueryHandler
from utils import load_and_clean_data
from config import CSV_FILE_PATH, TABLE_ID, PROJECT_ID, DATASET_ID, logger
from concurrent.futures import ThreadPoolExecutor

def load_data():
    """Loads and processes data before inserting into BigQuery."""
    df = load_and_clean_data(CSV_FILE_PATH)
    bq_handler = BigQueryHandler()

    logger.info("Setting up the dataset if needed...")
    bq_handler.create_dataset_if_not_exists()

    logger.info(f"Checking if '{TABLE_ID}' exists before inserting data...")
    if not bq_handler.check_table_exists(TABLE_ID):
        bq_handler.create_partitioned_table(TABLE_ID)

    logger.info(f"Inserting data into '{TABLE_ID}'...")
    bq_handler.load_dataframe_to_bq(df, TABLE_ID)

    # Process reporting period tables concurrently
    with ThreadPoolExecutor() as executor:
        executor.map(lambda period: process_reporting_period(bq_handler, period), range(1, 5))

def process_reporting_period(bq_handler, period):
    """Filters data for a reporting period and loads it into BigQuery."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` WHERE reporting_period = {period}"
    df_period = bq_handler.client.query(query).to_dataframe()
    table_id = f"TFL_JOURNEYS_TYPE_REPORTING_PERIOD_{period}"

    if not df_period.empty:
        logger.info(f"Verifying table '{table_id}' before inserting data...")
        if not bq_handler.check_table_exists(table_id):
            bq_handler.create_partitioned_table(table_id)

        logger.info(f"Loading data for Reporting Period {period}...")
        bq_handler.load_dataframe_to_bq(df_period, table_id)
