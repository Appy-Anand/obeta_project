from enum import Enum


class RawFileNames(str, Enum):
    pick_data = "pick_data.csv"
    product_details = "product_details.csv"
    warehouse_section = "warehouse_sections.csv"


class StagingFileNames(str, Enum):
    pick_data = "pick_data.parquet"
    product_details = "product_details.parquet"
    warehouse_section = "warehouse_sections.parquet"


class CurationFileNames(str, Enum):
    f_order_picks = "f_order_picks.parquet"
    f_returns = "f_returns.parquet"
    f_pick_errors = "f_pick_errors.parquet"
    d_date = "d_date.parquet"
    d_product_details = "d_product_details.parquet"
    d_warehouse_section = "d_warehouse_section.parquet"


class DataMartNames(str, Enum):
    total_pick_volume = "total_pick_volume.parquet"
    pick_volume_per_product_group = "pick_volume_per_product_group.parquet"
    order_volume = "order_volume.parquet"
    binned_order_volume = "binned_order_volume.parquet"
    weekly_zscore_distribution = "weekly_zscore_distribution.parquet"
    error_rate_per_warehouse_section = "error_rate_per_warehouse_section.parquet"
