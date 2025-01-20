from os import path, mkdir
import pandas as pd
from constants import (
    PICK_DATA_COLUMNS,
    GERMAN_PYTHON_ENCODING,
    PRODUCT_DETAILS_COLUMNS,
    PRODUCT_DETAILS_SCHEMA,
    PICK_DATA_SCHEMA,
    WAREHOUSE_SECTION_COLUMNS,
    WAREHOUSE_SECTION_SCHEMA,
    BASE_PATH,
)
from enums import RawFileNames, StagingFileNames

source_path = path.join(BASE_PATH, "source")
if not path.exists(source_path):
    raise FileNotFoundError(
        f"Source data cannot be found. Expected source csv data at location {BASE_PATH}"
    )
staging_path = path.join(BASE_PATH, "staging")
if not path.exists(staging_path):
    mkdir(staging_path)


def stage_pick_data():
    global staging_path, source_path
    csv_location = path.join(source_path, RawFileNames.pick_data)
    pick_data_df = pd.read_csv(
        csv_location,
        header=None,
        names=PICK_DATA_COLUMNS,
        encoding=GERMAN_PYTHON_ENCODING,
        dtype=PICK_DATA_SCHEMA,
    )
    pick_data_df["pick_timestamp"] = pd.to_datetime(
        pick_data_df["date"], format="%Y-%m-%d %H:%M:%S.%f"
    )
    pick_data_df["pick_date"] = pick_data_df["pick_timestamp"].dt.date
    pick_data_df = pick_data_df.drop(columns="date")
    write_path = path.join(staging_path, StagingFileNames.pick_data)
    pick_data_df.to_parquet(path=write_path, index=False)


def stage_product_details():
    global staging_path, source_path
    csv_location = path.join(source_path, RawFileNames.product_details)
    product_details_df = pd.read_csv(
        csv_location,
        header=None,
        names=PRODUCT_DETAILS_COLUMNS,
        dtype=PRODUCT_DETAILS_SCHEMA,
        encoding=GERMAN_PYTHON_ENCODING,
    )
    write_path = path.join(staging_path, StagingFileNames.product_details)
    product_details_df.to_parquet(path=write_path, index=False)


def stage_warehouse_sections():
    global staging_path, source_path
    csv_location = path.join(source_path, RawFileNames.warehouse_section)
    warehouse_sections_df = pd.read_csv(
        csv_location,
        header=None,
        names=WAREHOUSE_SECTION_COLUMNS,
        dtype=WAREHOUSE_SECTION_SCHEMA,
        encoding=GERMAN_PYTHON_ENCODING,
    )
    write_path = path.join(staging_path, StagingFileNames.warehouse_section)
    warehouse_sections_df.to_parquet(path=write_path, index=False)


if __name__ == "__main__":
    print("Starting Staging Transformations")
    stage_pick_data()
    print("Completed Pick Data")
    stage_product_details()
    print("Completed Product Details")
    stage_warehouse_sections()
    print("Completed Warehouse Section")
