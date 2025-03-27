from data_loader import load_data
from bigquery_handler import BigQueryHandler
from config import TABLE_ID, logger

def main():
    """Runs the data pipeline for loading and displaying BigQuery data."""
    logger.info("Starting the data pipeline...")

    # Load dataset into BigQuery
    load_data()

    # Initialize BigQuery handler
    bq_handler = BigQueryHandler()

    # Verify if the main table exists
    if not bq_handler.check_table_exists(TABLE_ID):
        logger.error(f"Table '{TABLE_ID}' is missing in BigQuery.")
        return

    # Show data from the main table
    bq_handler.display_table_data(TABLE_ID)

    # Process reporting period tables
    for period in range(1, 5):
        period_table = f"TFL_JOURNEYS_TYPE_REPORTING_PERIOD_{period}"
        if bq_handler.check_table_exists(period_table):
            bq_handler.display_table_data(period_table)
        else:
            logger.warning(f"Table '{period_table}' does not exist.")

    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
