from os import mkdir, path

import pandas as pd
from enums import StagingFileNames, CurationFileNames
from constants import BASE_PATH

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
    df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
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
    df.to_parquet(path.join(curation_path, CurationFileNames.d_date), index=False)
    return df


def curate_pick_data():
    global staging_path, curation_path
    picks_df = pd.read_parquet(path.join(staging_path, StagingFileNames.pick_data))
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

    error_df = picks_df[picks_df["pick_volume"] == 0].copy(deep=True)
    error_df.to_parquet(
        path.join(curation_path, CurationFileNames.f_pick_errors), index=False
    )

    returns_df = picks_df[picks_df["pick_volume"] < 0].copy(deep=True)
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

    picks_df[picks_df["pick_volume"] > 0].to_parquet(
        path.join(curation_path, CurationFileNames.f_order_picks), index=False
    )


def create_d_warehouse_section():
    global staging_path, curation_path
    stg_df = pd.read_parquet(
        path.join(staging_path, StagingFileNames.warehouse_section)
    )
    stg_df.to_parquet(path.join(curation_path, CurationFileNames.d_warehouse_section))


def create_d_product_details():
    global staging_path, curation_path
    stg_df = pd.read_parquet(path.join(staging_path, StagingFileNames.product_details))
    stg_df.to_parquet(path.join(curation_path, CurationFileNames.d_product_details))


if __name__ == "__main__":
    print("Starting Curation Transformations")
    create_d_date(start_date="2011-06-01", end_date="2020-07-15")
    print("Completed Date Dimension")
    curate_pick_data()
    print("Completed curating pick data")
    create_d_product_details()
    print("Completed d_product_details")
    create_d_warehouse_section()
    print("Done all curation tables")
