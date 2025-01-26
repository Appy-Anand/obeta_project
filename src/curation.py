"""
This script handles the **curation phase** of the ETL process,
where data from the staging layer is integrated, cleaned,
and transformed into a star schema format for analytics.

Key Responsibilities:
1. Read staged Parquet files from the staging layer.
2. Perform advanced cleaning and transformations:
   - Create surrogate keys for dimensions.
   - Normalize and standardize data.
   - Flag and handle errors (e.g., rows with zero or negative pick volumes).
3. Create dimension and fact tables:
   - Date dimension (d_date)
   - Product details dimension (d_product_details)
   - Warehouse sections dimension (d_warehouse_section)
   - Fact tables: f_order_picks, f_returns, and f_pick_errors.
4. Save curated datasets into the curation directory.

Outputs:
- Curated datasets ready for analytics and data mart generation.

Dependencies:
- Constants and configurations from `constants.py`.
- Logger utility for tracking ETL progress and errors.
"""
from os import mkdir, path

import pandas as pd
from enums import StagingFileNames, CurationFileNames
from constants import BASE_PATH
from utils import get_logger # Utility to initialize the logger

# Initialize logger for logging messages during curation transformations
logger = get_logger("curation")

staging_path = path.join(BASE_PATH, "staging")
if not path.exists(staging_path):
    raise FileNotFoundError(
        f"Staging data cannot be found. Expected staging parquet data at location {BASE_PATH}/staging"
    )
curation_path = path.join(BASE_PATH, "curation")
if not path.exists(curation_path):
    mkdir(curation_path)


def create_d_date(start_date: str, end_date: str) -> pd.DataFrame:
    global curation_path
    logger.info(f"Creating date dimension DataFrame from {start_date} to {end_date}")
    df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
    logger.debug(f"Generated date range with {len(df)} rows")
    df["year"] = df["date"].dt.year
    df["week"] = df["date"].dt.strftime("%Y_%W")
    df["month"] = df["date"].dt.strftime("%Y_%m")
    df["quarter"] = (
        df["date"].dt.year.astype(str) + "_Q" + df["date"].dt.quarter.astype(str)
    )
    df["year_half"] = (
        df["date"].dt.year.astype(str)
        + "_H"
        + ((df["date"].dt.quarter + 1) // 2).astype("str")
    )
    logger.debug("Derived columns: year, week, month, quarter, year_half")
    df.to_parquet(path.join(curation_path, CurationFileNames.d_date), index=False)
    logger.info(f"Date dimension DataFrame written to {curation_path}")
    logger.info("Date dimension DataFrame creation completed successfully")
    return df


def curate_pick_data():
    global staging_path, curation_path
    logger.info("Starting curation of pick data from staging to curated datasets.")
    # Load pick data from staging
    picks_df = pd.read_parquet(path.join(staging_path, StagingFileNames.pick_data))
    logger.info(f"Loaded pick data from {StagingFileNames.pick_data}, total rows: {len(picks_df)}.")

    # Create a surrogate key for order id
    picks_df["sk_order_id"] = (
        picks_df["order_number"].astype("str")
        + "_"
        + picks_df["pick_timestamp"].dt.strftime("%Y")
    )

    picks_df["sk_position_in_order"] = (
        picks_df.sort_values("pick_timestamp", ascending=True)
        .groupby(["sk_order_id", "origin"])
        .cumcount()
        + 1
    )
    logger.debug("Created surrogate keys: sk_order_id and sk_position_in_order.")

    # Separate rows with zero pick volume (errors)
    error_df = picks_df[picks_df["pick_volume"] == 0].copy(deep=True)
    logger.info(f"Identified {len(error_df)} rows with zero pick volume. Writing to {CurationFileNames.f_pick_errors}.")
    error_df.to_parquet(
        path.join(curation_path, CurationFileNames.f_pick_errors), index=False
    )


    # Separate rows with negative pick volume (returns)
    returns_df = picks_df[picks_df["pick_volume"] < 0].copy(deep=True)
    logger.info(f"Identified {len(returns_df)} rows with negative pick volume as returns. Writing to {CurationFileNames.f_returns}.")
    returns_df["pick_volume"] = (-1) * returns_df["pick_volume"]
    returns_df.rename(
        {
            "pick_volume": "return_volume",
            "pick_timestamp": "return_timestamp",
            "pick_date": "return_date",
        }
    )
    returns_df.to_parquet(
        path.join(curation_path, CurationFileNames.f_returns), index=False
    )

    # Process positive pick volumes
    positive_pick_df =picks_df[picks_df["pick_volume"] > 0]
    logger.info(f"Writing {len(positive_pick_df)} rows with positive pick volumes to {CurationFileNames.f_order_picks}.")
    positive_pick_df.to_parquet(
        path.join(curation_path, CurationFileNames.f_order_picks), index=False
    )
    logger.info("Curation of pick data completed successfully. Curated datasets: f_order_picks, f_pick_errors, and f_returns.")


def create_d_warehouse_section():
    global staging_path, curation_path
    logger.info("Starting creation of d_warehouse_section dimension table.")

    # Load data from staging
    try:
        stg_df = pd.read_parquet(
            path.join(staging_path, StagingFileNames.warehouse_section)
        )
        logger.info(f"Loading warehouse section data from {StagingFileNames.warehouse_section}.")
        logger.debug(f"Loaded {len(stg_df)} rows from staging data.")
    except FileNotFoundError as e:
        logger.error(f"Staging file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while loading staging data: {e}")
        raise
    # Check for empty data
    if stg_df.empty:
        logger.warning("Staging data for warehouse section is empty. No data will be written to curation.")
    # Write to curation
        try:
            stg_df.to_parquet(
                path.join(curation_path, CurationFileNames.d_warehouse_section), index=False
            )
            logger.info(f"Writing d_warehouse_section data to {CurationFileNames.d_warehouse_section}.")
        except Exception as e:
            logger.error(f"An error occurred while writing curation data: {e}")
            raise
    # Log completion
        logger.info("Creation of d_warehouse_section dimension table completed successfully.")


def create_d_product_details():
    global staging_path, curation_path
    logger.info("Starting creation of d_product_details dimension table.")
    # Load data from staging
    try:
        stg_df = pd.read_parquet(
            path.join(staging_path, StagingFileNames.product_details)
        )
        logger.info(f"Loading product details data from {StagingFileNames.product_details}.")
        logger.debug(f"Loaded {len(stg_df)} rows from staging data.")
    except FileNotFoundError as e:
        logger.error(f"Staging file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while loading staging data: {e}")
        raise


    # Check for empty data
    if stg_df.empty:
        logger.warning("Staging data for product details is empty. No data will be written to curation.")

    # Write to curation
    try:
        stg_df.to_parquet(
            path.join(curation_path, CurationFileNames.d_product_details), index=False
        )
        logger.info(f"Writing d_product_details data to {CurationFileNames.d_product_details}.")
    except Exception as e:
        logger.error(f"An error occurred while writing curation data: {e}")
        raise
    # Log function completion
    logger.info("Creation of d_product_details dimension table completed successfully.")


# Main block to run the Curation transformations
if __name__ == "__main__":
    logger.info("Starting Curation Transformations")
    create_d_date(start_date="2011-06-01", end_date="2020-07-15")
    logger.info("Completed Date Dimension")
    curate_pick_data()
    logger.info("Completed curating pick data")
    create_d_product_details()
    logger.info("Completed d_product_details")
    create_d_warehouse_section()
    logger.info("Done all curation tables")
