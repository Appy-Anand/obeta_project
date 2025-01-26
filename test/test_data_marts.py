# Import necessary libraries and functions for testing data marts
# pytest is used for parameterized and structured testing of data mart functions
# pandas is used to manipulate and compare dataframes
from os import path

import pandas as pd
import pytest


from src.data_marts import (pick_errors, pick_errors_w_drill_down, total_orders_processed,
                            total_orders_processed_w_drill_down, total_pick_volume, total_pick_volume_w_drill_down)



# Define reusable fixtures for mock and expected file paths, as well as mock datasets
@pytest.fixture
def mock_csv_path():
    return "/Users/aprajita/Desktop/APRAJITA_DA_PROJ/obeta-group-5/test/mock_data"



@pytest.fixture
def expected_csv_path():
    return "/Users/aprajita/Desktop/APRAJITA_DA_PROJ/obeta-group-5/test/expected_data"


@pytest.fixture
def pick_df(mock_csv_path):
    return pd.read_csv(path.join(mock_csv_path, "f_order_picks.csv"))


@pytest.fixture
def errors_df(mock_csv_path):
    return pd.read_csv(path.join(mock_csv_path, "f_pick_errors.csv"))


@pytest.fixture
def d_date(mock_csv_path):
    return pd.read_csv(path.join(mock_csv_path, "d_date.csv"))


def test_total_pick_volume(pick_df, d_date, expected_csv_path):
    df = total_pick_volume(pick_df, d_date)
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_pick_volume.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols), expected_df.sort_values(by=all_cols))



def test_total_pick_volume_w_product_group(pick_df, d_date, expected_csv_path):
    df = total_pick_volume_w_drill_down(pick_df, d_date, "product_group")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_pick_volume_w_product_group.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_pick_volume_w_origin(pick_df, d_date, expected_csv_path):
    df = total_pick_volume_w_drill_down(pick_df, d_date, "origin")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_pick_volume_w_origin.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_pick_volume_w_warehouse_section(pick_df, d_date, expected_csv_path):
    df = total_pick_volume_w_drill_down(pick_df, d_date, "warehouse_section")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_pick_volume_w_warehouse_section.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_orders_processed(pick_df, d_date, expected_csv_path):
    df = total_orders_processed(pick_df, d_date)
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_orders_processed.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_orders_processed_w_product_group(pick_df, d_date, expected_csv_path):
    df = total_orders_processed_w_drill_down(pick_df, d_date, "product_group")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_orders_processed_w_product_group.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_orders_processed_w_origin(pick_df, d_date, expected_csv_path):
    df = total_orders_processed_w_drill_down(pick_df, d_date, "origin")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_orders_processed_w_origin.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_total_orders_processed_w_warehouse_section(pick_df, d_date, expected_csv_path):
    df = total_orders_processed_w_drill_down(pick_df, d_date, "warehouse_section")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "total_orders_processed_w_warehouse_section.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_pick_errors(pick_df, errors_df, d_date, expected_csv_path):
    df = pick_errors(pick_df, errors_df, d_date)
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "pick_errors.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_pick_errors_w_product_group(pick_df, errors_df, d_date, expected_csv_path):
    df = pick_errors_w_drill_down(pick_df, errors_df, d_date, "product_group")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "pick_errors_w_product_group.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_pick_errors_w_origin(pick_df, errors_df, d_date, expected_csv_path):
    df = pick_errors_w_drill_down(pick_df, errors_df, d_date, "origin")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "pick_errors_w_origin.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))


def test_pick_errors_w_warehouse_section(pick_df, errors_df, d_date, expected_csv_path):
    df = pick_errors_w_drill_down(pick_df, errors_df, d_date, "warehouse_section")
    all_cols = list(df.columns)
    expected_df = pd.read_csv(path.join(expected_csv_path, "pick_errors_w_warehouse_section.csv"))
    pd.testing.assert_frame_equal(df.sort_values(by=all_cols).reset_index(drop=True),
                                  expected_df.sort_values(by=all_cols).reset_index(drop=True))
