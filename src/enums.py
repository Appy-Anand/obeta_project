# This script defines Enum classes for managing file names used in different stages of the ETL pipeline.
# Each class groups related file names for raw data, staging data, curated data, and data marts.

from enum import Enum

# Enum class to store file names for raw data files.
class RawFileNames(str, Enum):
    pick_data = "pick_data.csv"
    product_details = "product_details.csv"
    warehouse_section = "warehouse_sections.csv"

# Enum class to store file names for staging data files (transformed but not curated).
class StagingFileNames(str, Enum):
    pick_data = "pick_data.parquet"
    product_details = "product_details.parquet"
    warehouse_section = "warehouse_sections.parquet"


# Enum class to store file names for curated data files (processed and clean data).
class CurationFileNames(str, Enum):
    f_order_picks = "f_order_picks.parquet"
    f_returns = "f_returns.parquet"
    f_pick_errors = "f_pick_errors.parquet"
    d_date = "d_date.parquet"
    d_product_details = "d_product_details.parquet"
    d_warehouse_section = "d_warehouse_section.parquet"

# Enum class to store file names for data mart files (aggregated data for analytics).
class DataMartNames(str, Enum):
    total_pick_volume = "total_pick_volume.parquet"
    total_pick_volume_origin = "total_pick_volume_origin.parquet"
    total_pick_volume_product_group = "total_pick_volume_product_group.parquet"
    total_pick_volume_warehouse_section = "total_pick_volume_warehouse_section.parquet"
    total_orders_processed = "total_orders_processed.parquet"
    total_orders_processed_origin = "total_orders_processed_origin.parquet"
    total_orders_processed_product_group = "total_orders_processed_product_group.parquet"
    total_orders_processed_warehouse_section = "total_orders_processed_warehouse_section.parquet"
    pick_errors = "pick_errors.parquet"
    pick_errors_origin = "pick_errors_origin.parquet"
    pick_errors_product_group = "pick_errors_product_group.parquet"
    pick_errors_warehouse_section = "pick_errors_warehouse_section.parquet"
    top_n_products_weekly = "top_n_products_weekly.parquet"
    top_n_products_weekly_origin = "top_n_products_weekly_origin.parquet"
    top_n_products_weekly_product_group = "top_n_products_weekly_product_group.parquet"
    top_n_products_weekly_warehouse_section = "top_n_products_weekly_warehouse_section.parquet"
    pick_throughput = "pick_throughput.parquet"
    pick_throughput_origin = "pick_throughput_origin.parquet"
    pick_throughput_product_group = "pick_throughput_product_group.parquet"
    pick_throughput_warehouse_section = "pick_throughput_warehouse_section.parquet"
    order_mix = "order_mix.parquet"
    avg_products_picked_per_order = "avg_products_picked_per_order.parquet"
    order_count_by_type = "order_count_by_type.parquet"
    warehouse_utilization_per_section = "warehouse_utilization_per_section.parquet"
    binned_order_volume = "binned_order_volume.parquet"
    weekly_zscore_distribution = "weekly_zscore_distribution.parquet"


