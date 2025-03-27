import os
import logging
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config import PROJECT_ID, DATASET_ID, SERVICE_ACCOUNT_KEY

logger = logging.getLogger("BigQueryHandler")

class BigQueryHandler:
    def __init__(self):
        """Initializes the BigQuery client using service account credentials."""
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
        self.client = bigquery.Client(project=PROJECT_ID)

    def create_dataset_if_not_exists(self):
        """Creates a dataset if it doesn't already exist."""
        dataset_ref = self.client.dataset(DATASET_ID)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset '{DATASET_ID}' already exists.")
        except NotFound:
            logger.info(f"Creating dataset '{DATASET_ID}'...")
            self.client.create_dataset(DATASET_ID)

    def check_table_exists(self, table_id):
        """Checks if a specific table exists in BigQuery."""
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def create_partitioned_table(self, table_id):
        """Creates a partitioned and clustered table in BigQuery."""
        schema = [
            bigquery.SchemaField("reporting_period", "INTEGER"),
            bigquery.SchemaField("days_in_period", "INTEGER"),
            bigquery.SchemaField("period_beginning", "DATE"),
            bigquery.SchemaField("period_ending", "DATE"),
            bigquery.SchemaField("bus_journeys_m", "FLOAT"),
            bigquery.SchemaField("underground_journeys_m", "FLOAT"),
            bigquery.SchemaField("dlr_journeys_m", "FLOAT"),
            bigquery.SchemaField("tram_journeys_m", "FLOAT"),
            bigquery.SchemaField("overground_journeys_m", "FLOAT"),
            bigquery.SchemaField("london_cable_car_journeys_m", "FLOAT"),
            bigquery.SchemaField("tfl_rail_journeys_m", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]

        table_ref = self.client.dataset(DATASET_ID).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        table.clustering_fields = ["reporting_period"]

        try:
            self.client.create_table(table)
            logger.info(f"Table '{table_id}' created with partitioning.")
        except Exception as e:
            logger.warning(f"Table '{table_id}' already exists or failed to create: {e}")

    def load_dataframe_to_bq(self, df, table_id):
        """Loads a DataFrame into a BigQuery table."""
        try:
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

            if not self.check_table_exists(table_id):
                logger.info(f"Table '{table_id}' does not exist. Creating...")
                self.create_partitioned_table(table_id)

            if "timestamp" not in df.columns:
                df["timestamp"] = pd.to_datetime("now")

            job = self.client.load_table_from_dataframe(df, table_ref)
            job.result()
            logger.info(f"Successfully inserted {job.output_rows} rows into '{table_ref}'.")
        except Exception as e:
            logger.error(f"Failed to insert data into '{table_id}': {e}")

    def display_table_data(self, table_name):
        """Displays sample data from a BigQuery table."""
        if not self.check_table_exists(table_name):
            logger.warning(f"Table '{table_name}' not found.")
            return

        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` LIMIT 10"
        df = self.client.query(query).to_dataframe()
        logger.info(f"\nPreview of '{table_name}':\n{df}")
