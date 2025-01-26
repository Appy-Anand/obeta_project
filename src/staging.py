from os import path, mkdir
import pandas as pd
from constants import (
    PICK_DATA_COLUMNS,  # Column names for pick data CSV
    GERMAN_PYTHON_ENCODING,  # Encoding to handle special characters like German umlauts
    PRODUCT_DETAILS_COLUMNS,  # Column names for product details CSV
    PRODUCT_DETAILS_SCHEMA,  # Data schema for product details
    PICK_DATA_SCHEMA,  # Data schema for pick data
    WAREHOUSE_SECTION_COLUMNS,  # Column names for warehouse section CSV
    WAREHOUSE_SECTION_SCHEMA,  # Data schema for warehouse sections
    BASE_PATH,  # Base directory path for data
)
from enums import RawFileNames, StagingFileNames # Enum constants for file names
from utils import get_logger # Utility to initialize the logger

# Initialize logger for logging messages during staging transformations
logger = get_logger("staging")

# Define the source directory path
source_path = path.join(BASE_PATH, "source")
if not path.exists(source_path):
    # Raise an error if the source directory is missing
    raise FileNotFoundError(
        f"Source data cannot be found. Expected source csv data at location {BASE_PATH}"
    )

# Define the staging directory path
staging_path = path.join(BASE_PATH, "staging")
if not path.exists(staging_path):
    # Create the staging directory if it doesn't exist
    logger.info(f"Staging path not found. Creating folder: {staging_path}")
    mkdir(staging_path)



def stage_pick_data():
    """
       Reads the raw pick data from a CSV file, processes it, and saves it as a Parquet file in the staging area.
    """
    global staging_path, source_path
    # Construct the file path for the raw pick data CSV
    csv_location = path.join(source_path, RawFileNames.pick_data)
    logger.info("Attempting to read raw pick data")

    # Read the raw CSV file into a Pandas DataFrame
    pick_data_df = pd.read_csv(
        csv_location,
        header=None,  # No header in the raw file; column names will be assigned
        names=PICK_DATA_COLUMNS,  # Assign pre-defined column names
        encoding=GERMAN_PYTHON_ENCODING,  # Handle special characters like German umlauts
        dtype=PICK_DATA_SCHEMA,  # Enforce data types for each column
    )
    # Parse the 'date' column into a datetime object
    logger.info("Parsing timestamp from pick data.")
    pick_data_df["pick_timestamp"] = pd.to_datetime(
        pick_data_df["date"], format="%Y-%m-%d %H:%M:%S.%f"
    )
    # Extract the date (YYYY-MM-DD) from the timestamp
    pick_data_df["pick_date"] = pick_data_df["pick_timestamp"].dt.date

    # Drop the original 'date' column as it's no longer needed
    pick_data_df = pick_data_df.drop(columns="date")
    logger.info("Creating a full file path to save transformed data")

    # Construct the file path for saving the processed data
    write_path = path.join(staging_path, StagingFileNames.pick_data)
    logger.info("Saving the cleaned and processed DataFrame (pick_data_df) as a Parquet file at the specified path")

    # Save the processed DataFrame as a Parquet file
    pick_data_df.to_parquet(path=write_path, index=False)

# Function to stage product details data
def stage_product_details():
    """
        Reads the raw product details from a CSV file, processes it, and saves it as a Parquet file in the staging area.
    """
    global staging_path, source_path

    # Construct the file path for the raw product details CSV
    csv_location = path.join(source_path, RawFileNames.product_details)
    logger.info("Attempting to read raw product details data")
    # Read the raw CSV file into a Pandas DataFrame
    product_details_df = pd.read_csv(
        csv_location,
        header=None,  # No header in the raw file; column names will be assigned
        names=PRODUCT_DETAILS_COLUMNS,  # Assign pre-defined column names
        dtype=PRODUCT_DETAILS_SCHEMA,  # Enforce data types for each column
        encoding=GERMAN_PYTHON_ENCODING,  # Handle special characters
    )
    logger.info("Creating a full file path to save transformed data")
    # Construct the file path for saving the processed data
    write_path = path.join(staging_path, StagingFileNames.product_details)
    logger.info("Saving the cleaned and processed DataFrame (product_details_df) as a Parquet file at the specified path")
    # Save the processed DataFrame as a Parquet file
    product_details_df.to_parquet(path=write_path, index=False)


def stage_warehouse_sections():
    """
    Reads the raw warehouse sections data from a CSV file, processes it, and saves it as a Parquet file in the staging area.
    """
    global staging_path, source_path

    # Construct the file path for the raw warehouse sections CSV
    csv_location = path.join(source_path, RawFileNames.warehouse_section)

    # Read the raw CSV file into a Pandas DataFrame
    warehouse_sections_df = pd.read_csv(
        csv_location,
        header=None,  # No header in the raw file; column names will be assigned
        names=WAREHOUSE_SECTION_COLUMNS,  # Assign pre-defined column names
        dtype=WAREHOUSE_SECTION_SCHEMA,  # Enforce data types for each column
        encoding=GERMAN_PYTHON_ENCODING,  # Handle special characters
    )
    logger.info("Creating a full file path to save transformed data")
    # Construct the file path for saving the processed data
    write_path = path.join(staging_path, StagingFileNames.warehouse_section)
    logger.info("Saving the cleaned and processed DataFrame (warehouse_sections_df) as a Parquet file at the specified path")
    # Save the processed DataFrame as a Parquet file
    warehouse_sections_df.to_parquet(path=write_path, index=False)

# Main block to run the staging transformations
if __name__ == "__main__":
    logger.info("Starting Staging Transformations")
    stage_pick_data()
    logger.info("Completed Pick Data")
    stage_product_details()
    logger.info("Completed Product Details")
    stage_warehouse_sections()
    logger.info("Completed Warehouse Section")
