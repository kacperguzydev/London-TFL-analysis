import pandas as pd
from config import logger

def load_and_clean_data(file_path):
    """Loads and cleans CSV data before inserting into BigQuery."""
    logger.info(f"Loading data from {file_path}...")
    df = pd.read_csv(file_path)

    # ✅ Print actual column names from the CSV
    logger.info(f"Original Column Names: {list(df.columns)}")

    df.drop_duplicates(inplace=True)

    # ✅ Standardize column names for BigQuery
    df.columns = (
        df.columns.str.replace(' ', '_')  # Replace spaces with underscores
        .str.replace('[^a-zA-Z0-9_]', '', regex=True)  # Remove special characters
        .str.lower()  # Convert to lowercase
    )

    logger.info(f"Updated Column Names: {list(df.columns)}")

    # ✅ Convert date columns to proper datetime format
    date_columns = ["period_beginning", "period_ending"]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d-%b-%y", errors='coerce')
            logger.info(f"Converted column {col} to datetime.")
        else:
            logger.warning(f"Column {col} not found in dataset!")

    # ✅ Convert numeric columns to float
    numeric_columns = ["bus_journeys_m", "underground_journeys_m", "dlr_journeys_m",
                       "tram_journeys_m", "overground_journeys_m", "london_cable_car_journeys_m", "tfl_rail_journeys_m"]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            logger.warning(f"Column '{col}' not found in dataset!")

    # ✅ Remove period_and_financial_year if it's not part of the schema
    if "period_and_financial_year" in df.columns:
        df.drop(columns=["period_and_financial_year"], inplace=True)
        logger.info("Removed column 'period_and_financial_year' to match BigQuery schema.")

    logger.info("Data cleaning completed.")
    return df
