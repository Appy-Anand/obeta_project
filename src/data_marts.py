"""
This script defines data mart generation functions to transform and aggregate curated data for analytics.
Each function processes specific KPIs and generates Parquet files for consumption by analytics tools.
"""
import os
from os import mkdir, path

import duckdb
import pandas as pd

from src.constants import BASE_PATH
from src.enums import CurationFileNames, DataMartNames
from src.utils import get_logger  # Utility to initialize the logger

# Initialize logger for logging messages during staging transformations
logger = get_logger("data_mart")

curation_path = path.join(BASE_PATH, "curation")
if not path.exists(curation_path):
    raise FileNotFoundError(f"Curation data cannot be found. Expected curation data at location {BASE_PATH}/curation")
data_mart_path = path.join(BASE_PATH, "data_mart")
if not path.exists(data_mart_path):
    mkdir(data_mart_path)

# Common dimensions used for drill-downs in data mart calculations
# These columns represent key groupings for aggregations, enabling
# detailed analytics by:
# - `product_group`: Groups by the type/category of products
# - `origin`: Differentiates between store orders (46) and customer orders (48)
# - `warehouse_section`: Identifies the specific warehouse section used
common_drill_downs = ["product_group", "origin", "warehouse_section"]


def total_pick_volume(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
     Calculates the total pick volume by date, and performs drill-downs by
     product group, origin, and warehouse section. Saves the results as Parquet files.

     Args:
         f_order_picks (pd.DataFrame): Curated order pick data
         d_date (pd.DataFrame): Date dimension data
     Returns:
        None: Writes aggregated data to Parquet files.
     """
    logger.info("Starting to process function: total_pick_volume")
    global data_mart_path

    # Ensure output directory exists
    total_pick_volume_path = path.join(data_mart_path, "total_pick_volume")
    if not path.exists(total_pick_volume_path):
        os.mkdir(total_pick_volume_path)

    agg_df = duckdb.sql("""
                WITH agg_cte AS (
                    SELECT 
                        pick_date,
                        sum(pick_volume) AS pick_volume
                    FROM
                        f_order_picks
                    GROUP BY f_order_picks.pick_date
                )
                SELECT
                    d_date.date,
                    coalesce(agg_cte.pick_volume, 0) as pick_volume,
                    d_date.week,
                    d_date.month,
                    d_date.quarter,
                    d_date.year_half,
                    d_date.year
                FROM
                    agg_cte
                LEFT JOIN
                    d_date
                ON
                    d_date.date == agg_cte.pick_date
                ORDER BY d_date.date
            """).df()
    logger.info(f"Done processing total_pick_volume. Initiating write.")
    # Save aggregated data as Parquet
    agg_df.to_parquet(path.join(total_pick_volume_path, "total_pick_volume.parquet"))
    logger.info(f"Processed function: total_pick_volume.")
    return agg_df


def total_pick_volume_w_drill_down(f_order_picks: pd.DataFrame, d_date: pd.DataFrame, drill_down: str) -> pd.DataFrame:
    """
     Calculates the total pick volume by date, and performs drill-downs by
     product group, origin, and warehouse section. Saves the results as Parquet files.

     Args:
         f_order_picks (pd.DataFrame): Curated order pick data
         d_date (pd.DataFrame): Date dimension data
     Returns:
        None: Writes aggregated data to Parquet files.
     """
    logger.info(f"Starting to process function: total_pick_volume for drill down: {drill_down}")
    global data_mart_path

    # Ensure output directory exists
    total_pick_volume_path = path.join(data_mart_path, "total_pick_volume")
    if not path.exists(total_pick_volume_path):
        os.mkdir(total_pick_volume_path)

    # Process drill-downs
    sql_script = f"""
                    WITH agg_cte AS (
                        SELECT 
                            pick_date,
                            {drill_down},
                            sum(pick_volume) AS pick_volume
                        FROM
                            f_order_picks
                        GROUP BY 1, 2
                    )
                    SELECT
                        d_date.date,
                        {drill_down},
                        coalesce(agg_cte.pick_volume, 0) as pick_volume,
                        d_date.week,
                        d_date.month,
                        d_date.quarter,
                        d_date.year_half,
                        d_date.year
                    FROM
                        agg_cte
                    LEFT JOIN
                        d_date
                    ON
                        d_date.date == agg_cte.pick_date
                    ORDER BY d_date.date
        """
    agg_df = duckdb.sql(sql_script).df()
    logger.info(f"Done processing total_pick_volume for drill down: {drill_down}. Initiating write.")
    agg_df.to_parquet(path.join(total_pick_volume_path, f"total_pick_volume_{drill_down}.parquet"))
    logger.info(f"Processed function: total_pick_volume for drill down: {drill_down}")
    return agg_df


def total_orders_processed(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the total number of distinct orders processed per day and generates drill-downs
    for specific dimensions (product group, origin, warehouse section).

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: total_orders_processed")
    global data_mart_path

    # Define output path for the aggregated data
    total_orders_processed_path = path.join(data_mart_path, "total_orders_processed")
    if not path.exists(total_orders_processed_path):
        os.mkdir(total_orders_processed_path)

    # Aggregate total orders processed by date using SQL
    agg_df = duckdb.sql("""
           WITH agg_cte AS (
                SELECT 
                    pick_date,
                    COUNT(DISTINCT(sk_order_id)) AS order_volume
                FROM
                    f_order_picks
                GROUP BY f_order_picks.pick_date
            )
            SELECT
                d_date.date,
                coalesce(agg_cte.order_volume, 0) as order_volume,
                d_date.week,
                d_date.month,
                d_date.quarter,
                d_date.year_half,
                d_date.year
            FROM
                d_date
            LEFT JOIN
                agg_cte
            ON
                d_date.date == agg_cte.pick_date
            ORDER BY d_date.date

        """).df()
    logger.info("Done processing total_orders_processed. Initiating write.")
    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(total_orders_processed_path, "total_orders_processed.parquet"))
    logger.info("Processed function: total_orders_processed")
    return agg_df


def total_orders_processed_w_drill_down(f_order_picks: pd.DataFrame, d_date: pd.DataFrame,
                                        drill_down: str) -> pd.DataFrame:
    """
    Calculates the total number of distinct orders processed per day and generates drill-downs
    for specific dimensions (product group, origin, warehouse section).

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info(f"Starting to process function: total_orders_processed for drill down: {drill_down}")
    global data_mart_path

    # Define output path for the aggregated data
    total_orders_processed_path = path.join(data_mart_path, "total_orders_processed")
    if not path.exists(total_orders_processed_path):
        os.mkdir(total_orders_processed_path)

    # SQL query to calculate total orders processed for each drill-down dimension
    sql_script = f"""
               WITH agg_cte AS (
                   SELECT 
                       pick_date,
                       {drill_down},
                       COUNT(DISTINCT(sk_order_id)) AS order_volume
                   FROM
                       f_order_picks
                   GROUP BY f_order_picks.pick_date, {drill_down}
               )
               SELECT
                   d_date.date,
                   {drill_down},
                   coalesce(agg_cte.order_volume, 0) as order_volume,
                   d_date.week,
                   d_date.month,
                   d_date.quarter,
                   d_date.year_half,
                   d_date.year
               FROM
                   d_date
               LEFT JOIN
                   agg_cte
               ON
                   d_date.date == agg_cte.pick_date
               ORDER BY d_date.date

           """
    logger.debug(f"Executing SQL query for drill-down: {drill_down}")
    agg_df = duckdb.sql(sql_script).df()
    logger.info(f"Done processing total_orders_processed for drill down: {drill_down}. Initiating write.")
    agg_df.to_parquet(path.join(total_orders_processed_path, f"total_orders_processed_{drill_down}.parquet"))
    logger.info(f"Done processing function total_orders_processed for drill down: {drill_down}")
    return agg_df


def pick_errors(f_order_picks: pd.DataFrame, f_pick_errors: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
      Calculates the total number of pick errors and total picks by date,
      with drill-downs for specific dimensions (e.g., product group, origin, warehouse section).
      Saves aggregated results as Parquet files for analytics.

      Args:
          f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
          f_pick_errors (pd.DataFrame): DataFrame containing curated pick error data.
          d_date (pd.DataFrame): DataFrame containing date dimension data.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: pick_errors")
    global data_mart_path

    # Ensure the output directory exists
    pick_errors_path = path.join(data_mart_path, "pick_errors")
    if not path.exists(pick_errors_path):
        os.mkdir(pick_errors_path)

    # Aggregate total pick errors and total picks by date, week, and month using SQL
    agg_df = duckdb.sql("""
            with total_errors_cte as (
            select 
                d_date.date,
                d_date.week,
                d_date.month,
                count(*) as total_errors
            from f_pick_errors as pe
            left join d_date
                on pe.pick_date = d_date.date
            group by 1, 2, 3
        ),
        total_picks_cte as (
            select 
                d_date.date,
                d_date.week,
                d_date.month,
                count(*) as total_picks
            from f_order_picks as op
            left join d_date
                on op.pick_date = d_date.date
            group by 1, 2, 3
        )
        select 
            total_errors_cte.*,
            total_picks_cte.total_picks 
        from total_errors_cte
        left join total_picks_cte
        on total_errors_cte.date = total_picks_cte.date
        and total_errors_cte.week = total_picks_cte.week
        and total_errors_cte.month = total_picks_cte.month;


        """).df()
    logger.info(f"Done processing pick_errors. Initiating write.")
    # Save aggregated data as a Parquet file
    agg_df.to_parquet(path.join(pick_errors_path, "pick_errors.parquet"))
    logger.info(f"Processed function: pick_errors")
    return agg_df


def pick_errors_w_drill_down(f_order_picks: pd.DataFrame, f_pick_errors: pd.DataFrame, d_date: pd.DataFrame,
                             drill_down: str) -> pd.DataFrame:
    """
      Calculates the total number of pick errors and total picks by date,
      with drill-downs for specific dimensions (e.g., product group, origin, warehouse section).
      Saves aggregated results as Parquet files for analytics.

      Args:
          f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
          f_pick_errors (pd.DataFrame): DataFrame containing curated pick error data.
          d_date (pd.DataFrame): DataFrame containing date dimension data.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info(f"Starting to process function: pick_errors for drill down: {drill_down}")
    global data_mart_path

    # Ensure the output directory exists
    pick_errors_path = path.join(data_mart_path, "pick_errors")
    if not path.exists(pick_errors_path):
        os.mkdir(pick_errors_path)

    # SQL query to calculate pick errors and total picks for the current drill-down dimension
    sql_script = f"""
        with total_errors_cte as (
            select 
                d_date.date,
                d_date.week,
                d_date.month,
                pe.{drill_down},
                count(*) as total_errors
            from f_pick_errors as pe
            left join d_date
                on pe.pick_date = d_date.date
            group by 1, 2, 3, 4
        ),
        total_picks_cte as (
            select 
                d_date.date,
                d_date.week,
                d_date.month,
                op.{drill_down},
                count(*) as total_picks
            from f_order_picks as op
            left join d_date
                on op.pick_date = d_date.date
            group by 1, 2, 3, 4
        )
        select 
            total_errors_cte.*,
            total_picks_cte.total_picks 
        from total_errors_cte
        left join total_picks_cte
        on total_errors_cte.date = total_picks_cte.date
        and total_errors_cte.week = total_picks_cte.week
        and total_errors_cte.month = total_picks_cte.month
        and total_errors_cte.{drill_down} = total_picks_cte.{drill_down};

        """
    logger.debug(f"Executing SQL query for drill-down: {drill_down}")
    agg_df = duckdb.sql(sql_script).df()
    logger.info(f"Done processing pick_errors for drill down: {drill_down}. Initiating write.")
    # Save the drill-down results as a Parquet file
    agg_df.to_parquet(path.join(pick_errors_path, f"pick_errors_{drill_down}.parquet"))
    logger.info(f"Processed function: pick_errors for drill down: {drill_down}")
    return agg_df


def top_n_products_weekly(f_order_picks: pd.DataFrame, d_date: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Identifies the top N products by total picks per week. Performs drill-downs
    on specific dimensions (e.g., product group, origin, warehouse section) and
    saves the aggregated results as Parquet files.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
        n (int): Number of top products to retrieve.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info(f"Starting to process function: top_n_products_weekly for n = {n}")
    global data_mart_path

    # Ensure the output directory exists
    top_n_products_weekly_path = path.join(data_mart_path, "top_n_products_weekly")
    if not path.exists(top_n_products_weekly_path):
        os.mkdir(top_n_products_weekly_path)

    logger.info("Executing SQL query to calculate weekly top N products.")
    # SQL query to calculate total picks per product and rank them weekly
    agg_df = duckdb.sql(f"""
         with total_picks_cte as (
        select 
            d_date.week,
            product_id,
            count(*) as total_picks
        from f_order_picks
        left join d_date
            on f_order_picks.pick_date = d_date.date
        group by 1, 2
    ),
    ranked_picks_cte as (
        select 
            total_picks_cte.*,
            row_number() over (partition by week order by total_picks desc) as rank
        from total_picks_cte
    )
    select 
        week,
        product_id,
        total_picks
    from ranked_picks_cte
    where rank <= {n}
        """).df()
    logger.info("Done executing SQL query. Initiating write.")
    # Save the top N products data as a Parquet file
    agg_df.to_parquet(path.join(top_n_products_weekly_path, "top_n_products_weekly.parquet"))
    logger.info("Processed function: top_n_products_weekly")
    return agg_df


def top_n_products_weekly_w_drill_down(f_order_picks: pd.DataFrame, d_date: pd.DataFrame, drill_down: str,
                                       n: int = 10) -> pd.DataFrame:
    """
    Identifies the top N products by total picks per week. Performs drill-downs
    on specific dimensions (e.g., product group, origin, warehouse section) and
    saves the aggregated results as Parquet files.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
        n (int): Number of top products to retrieve.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info(f"Starting to process function: top_n_products_weekly for drill down {drill_down}")
    global data_mart_path
    n = 10  # Define the number of top products to retrieve

    # Ensure the output directory exists
    top_n_products_weekly_path = path.join(data_mart_path, "top_n_products_weekly")
    if not path.exists(top_n_products_weekly_path):
        os.mkdir(top_n_products_weekly_path)

    # SQL query to calculate top N products by weekly total picks with drill-down
    sql_script = f"""
           with total_picks_cte as (
               select 
                   d_date.week,
                   product_id,
                   {drill_down},
                   count(*) as total_picks
               from f_order_picks
               left join d_date
                   on f_order_picks.pick_date = d_date.date
               group by 1, 2, 3
           ),
           ranked_picks_cte as (
               select 
                   total_picks_cte.*,
                   row_number() over (partition by week, {drill_down} order by total_picks desc) as rank
               from total_picks_cte
           )
           select 
               week,
               product_id,
               {drill_down},
               total_picks
           from ranked_picks_cte
           where rank <= {n}
            """
    agg_df = duckdb.sql(sql_script).df()
    # Save the drill-down results as a Parquet file
    agg_df.to_parquet(path.join(top_n_products_weekly_path, f"top_n_products_weekly_{drill_down}.parquet"))
    logger.info(f"Done processing top_n_products_weekly for drill down: {drill_down}")
    return agg_df


def avg_products_picked_per_order(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the average number of unique products picked per order for each week.
    Aggregates results and saves them as Parquet files for further analysis.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
     Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: avg_products_picked_per_order")
    global data_mart_path

    # SQL query to calculate the average number of unique products picked per order
    logger.debug("Executing SQL query to calculate weekly average of unique products picked per order.")
    agg_df = duckdb.sql("""
        with order_distribution_cte as (
            select 
                sk_order_id,
                min(pick_date) as order_date,
                count(distinct product_id) as unique_product_count
            from f_order_picks
            group by 1
        ),
        daily_distribution as (
            select 
                d_date.week,
                avg(unique_product_count) as avg_products_picked_per_order
            from order_distribution_cte
            left join d_date
                on order_distribution_cte.order_date = d_date.date
            group by 1
            order by 1 asc
        )
        select 
            *
        from daily_distribution
        """).df()
    # Ensure the output directory exists
    avg_products_picked_per_order_path = path.join(data_mart_path, "avg_products_picked_per_order")
    if not path.exists(avg_products_picked_per_order_path):
        os.mkdir(avg_products_picked_per_order_path)

    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(avg_products_picked_per_order_path, "avg_products_picked_per_order.parquet"))
    logger.info("Processed function: avg_products_picked_per_order")
    return agg_df


def order_count_by_type(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the weekly order volume split by origin type (46: store orders, 48: customer orders).
    Also calculates total order volume, order percentages, and ratios between the two types.
    Saves the aggregated results as a Parquet file for analytics.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.
    Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: order_count_by_type")
    global data_mart_path

    # SQL query to calculate weekly order counts by origin type (46 and 48)
    agg_df = duckdb.sql("""
            WITH cte_48 AS (
                SELECT 
                    d_date.week,
                    COUNT(DISTINCT(sk_order_id)) AS order_volume
                FROM
                    f_order_picks
                left join d_date
                    on f_order_picks.pick_date = d_date.date
                where
                    f_order_picks.origin = '48'
                GROUP BY 1
            ),
            cte_46 AS (
                SELECT 
                    d_date.week,
                    COUNT(DISTINCT(sk_order_id)) AS order_volume
                FROM
                    f_order_picks
                left join d_date
                    on f_order_picks.pick_date = d_date.date
                where
                    f_order_picks.origin = '46'
                GROUP BY 1
            ),
            all_orders as ( 
                SELECT
                    COALESCE(cte_48.week, cte_46.week) as week,
                    COALESCE(cte_48.order_volume, 0) as order_volume_48,
                    COALESCE(cte_46.order_volume, 0) as order_volume_46,
                    cte_48.order_volume + cte_46.order_volume as total_order_volume,
                    round(cte_48.order_volume / (cte_48.order_volume + cte_46.order_volume), 4) * 100 as order_percentage_48,
                    round(cte_46.order_volume / (cte_48.order_volume + cte_46.order_volume), 4) * 100 as order_percentage_46
                FROM
                    cte_48
                FULL OUTER JOIN
                    cte_46
                ON
                    cte_48.week = cte_46.week
            )
            select
                all_orders.*,
                case when order_volume_48 > 0 then round(order_volume_46 / order_volume_48, 2) else 0 end as ratio_46_48,
                case when order_volume_46 > 0 then round(order_volume_48 / order_volume_46, 2) else 0 end as ratio_48_46
            from
                all_orders
        """).df()

    # Ensure the output directory exists
    order_count_by_type_path = path.join(data_mart_path, "order_count_by_type")
    if not path.exists(order_count_by_type_path):
        os.mkdir(order_count_by_type_path)

    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(order_count_by_type_path, "order_count_by_type.parquet"))
    logger.info("Processed function: order_count_by_type")
    return agg_df


def warehouse_utilization_per_section(d_date: pd.DataFrame, f_order_picks: pd.DataFrame, ) -> pd.DataFrame:
    """
    Calculates the weekly warehouse utilization percentage per section by comparing the
    pick volume for each section against the total pick volume. Saves the results as
    a Parquet file for analytics.

    Args:
        d_date (pd.DataFrame): DataFrame containing date dimension data.
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.

    Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: warehouse_utilization_per_section")
    global data_mart_path

    # SQL query to calculate weekly warehouse utilization percentage per section
    agg_df = duckdb.sql("""
            WITH section_agg AS (
                SELECT 
                    d_date.week,
                    warehouse_section,
                    sum(pick_volume) AS pick_volume
                FROM
                    f_order_picks
                LEFT JOIN
                    d_date
                on f_order_picks.pick_date = d_date.date
                GROUP BY 1, 2
            ),
            total_agg AS (
                SELECT 
                    d_date.week,
                    sum(pick_volume) AS pick_volume
                FROM
                    f_order_picks
                LEFT JOIN
                    d_date
                on f_order_picks.pick_date = d_date.date
                GROUP BY 1
            )
            select 
                total_agg.week,
                section_agg.warehouse_section,
                (round(
                    coalesce(section_agg.pick_volume, 0) / total_agg.pick_volume,
                    4 
                ) * 100) as section_utilization
            from total_agg
            left join section_agg
            on total_agg.week = section_agg.week
            order by 1, 2 desc
        """).df()
    # Ensure the output directory exists
    warehouse_utilization_per_section_path = path.join(data_mart_path, "warehouse_utilization_per_section")
    if not path.exists(warehouse_utilization_per_section_path):
        os.mkdir(warehouse_utilization_per_section_path)

    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(warehouse_utilization_per_section_path, "warehouse_utilization_per_section.parquet"))
    logger.info("Processed function: warehouse_utilization_per_section")
    return agg_df


def pick_throughput(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates hourly pick volumes and weekly average pick throughput for each warehouse section,
    origin, and product group. Performs drill-down analysis for specified dimensions
    and saves the results as Parquet files.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.

    Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info("Starting to process function: pick_throughput")
    global data_mart_path

    # Ensure the output directory exists
    pick_throughput_path = path.join(data_mart_path, "pick_throughput")
    if not path.exists(pick_throughput_path):
        os.mkdir(pick_throughput_path)

    logger.info("Initiating processing pick_throughput")
    # SQL query to calculate hourly pick volumes and weekly averages
    agg_df = duckdb.sql("""
            WITH hourly_agg_cte AS (
                SELECT 
                    pick_date,
                    hour(f_order_picks.pick_timestamp) as pick_hour,
                    sum(pick_volume) AS pick_volume
                FROM
                    f_order_picks
                GROUP BY 1, 2
            ), 
            weekly_avg as (
                select 
                    d_date.week,
                    f_order_picks.origin,
                    f_order_picks.warehouse_section,
                    f_order_picks.product_group,
                    round(avg(pick_volume), 2) as weekly_pick_throughput_avg  
                from hourly_agg_cte
                left join d_date
                    on hourly_agg_cte.pick_date = d_date.date
                group by 1
            )
            select * from hourly_agg_cte
        """).df()
    logger.info("Processing complete. Initiating write.")
    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(pick_throughput_path, "pick_throughput.parquet"))
    logger.info("Processed function: pick_throughput.")
    return agg_df


def pick_throughput_w_drill_down(f_order_picks: pd.DataFrame, d_date: pd.DataFrame, drill_down: str) -> pd.DataFrame:
    """
    Calculates hourly pick volumes and weekly average pick throughput for each warehouse section,
    origin, and product group. Performs drill-down analysis for specified dimensions
    and saves the results as Parquet files.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.

    Returns:
        None: Writes aggregated data to Parquet files.
    """
    logger.info(f"Starting to process function: pick_throughput for drill down: {drill_down}")
    global data_mart_path

    # Ensure the output directory exists
    pick_throughput_path = path.join(data_mart_path, "pick_throughput")
    if not path.exists(pick_throughput_path):
        os.mkdir(pick_throughput_path)

    logger.info(f"Starting to process drill down: {drill_down}")
    # SQL query to calculate pick throughput with drill-down dimensions
    sql_script = f"""
                    WITH hourly_agg_cte AS (
                        SELECT 
                            pick_date,
                            hour(f_order_picks.pick_timestamp) as pick_hour,
                            {drill_down},
                            sum(pick_volume) AS pick_volume
                        FROM
                            f_order_picks
                        GROUP BY 1, 2, 3
                    ), 
                    weekly_avg as (
                        select 
                            d_date.week,
                            {drill_down},
                            round(avg(pick_volume), 2) as weekly_pick_throughput_avg
                        from hourly_agg_cte
                        left join d_date
                            on hourly_agg_cte.pick_date = d_date.date
                        group by 1, 2
                    )
                    select * from weekly_avg
        """
    # Save the drill-down results as a Parquet file
    agg_df = duckdb.sql(sql_script).df()
    logger.info("Completed processing. Initiating write")
    agg_df.to_parquet(path.join(pick_throughput_path, f"pick_throughput_{drill_down}.parquet"))
    logger.info(f"Processed function: pick_throughput for drill down: {drill_down}")
    return agg_df


def binned_order_volume(f_order_picks: pd.DataFrame, d_date: pd.DataFrame) -> pd.DataFrame:
    """
    Categorizes orders into bins based on their pick volume and generates a time-series
    dataset with order volumes per category. Saves the results as a Parquet file.

    Args:
        f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
        d_date (pd.DataFrame): DataFrame containing date dimension data.

    Returns:
        None: Writes aggregated data to a Parquet file.
    """
    logger.info("Starting to process function: binned_order_volume")
    global data_mart_path

    # Step 1: Aggregate pick volumes by order and date
    logger.debug("Executing SQL query to aggregate pick volumes by order and date.")
    raw_order_df = duckdb.sql("""
        select
            pick_date,
            sk_order_id,
            sum(pick_volume) as pick_volume,
        from f_order_picks
        group by 1, 2
        """).df()

    # Step 2: Define bins and labels for categorizing pick volumes
    bins = [0, 50, 150, 350, 600, 900, 200000]
    labels = ["mini", "small", "medium", "large", "extra_large", "extreme"]
    labels_df = pd.DataFrame({"bins": labels})
    raw_order_df["bins"] = pd.cut(raw_order_df["pick_volume"], bins=bins, labels=labels)

    # Step 3: Aggregate binned data into a time-series format
    logger.debug("Executing SQL query to aggregate binned data into a time-series format.")
    time_series_df = duckdb.sql("""
        with agg_cte as(
            select
                pick_date,
                bins,
                count(1) as order_volume
            from raw_order_df
            group by pick_date, bins
        )
        select
            d_date.date,
            labels_df.bins,
            coalesce(agg_cte.order_volume, 0) as order_volume,
            d_date.week,
            d_date.month,
            d_date.quarter,
            d_date.year_half
        from d_date
        cross join 
            labels_df
        left join agg_cte
        on 
            d_date.date == agg_cte.pick_date
            and labels_df.bins == agg_cte.bins
        """).df()
    # Ensure the output directory exists
    binned_order_volume_path = path.join(data_mart_path, "binned_order_volume")
    if not path.exists(binned_order_volume_path):
        os.mkdir(binned_order_volume_path)

    # Step 4: Save the time-series data as a Parquet file
    time_series_df.rename({"bins": "category"})
    time_series_df.to_parquet(path.join(data_mart_path, DataMartNames.binned_order_volume))
    logger.info("Processed function: binned_order_volume")
    return time_series_df


def weekly_zscore_distribution(f_order_picks: pd.DataFrame, d_date: pd.DataFrame,
                               z_score_group: str = "week") -> pd.DataFrame:
    """
        Computes the z-score distribution of pick volumes for each aggregation period
        (e.g., week) and saves the results as a Parquet file.

        Args:
            f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.
            d_date (pd.DataFrame): DataFrame containing date dimension data.
            z_score_group (str): The column to group by for z-score computation (default is "week").

        Returns:
            None: Writes aggregated data with z-scores to a Parquet file.
        """
    logger.info("Starting to process function: weekly_zscore_distribution")

    # SQL query to calculate z-scores for pick volumes grouped by the specified aggregation period
    logger.info(f"Executing SQL query for z-score distribution grouped by '{z_score_group}'.")
    agg_df = duckdb.sql(f"""
        with orders as (
            select
                f_order_picks.sk_order_id,
                d_date.{z_score_group},
                sum(f_order_picks.pick_volume) as pick_volume
            from f_order_picks
            left join d_date
                on f_order_picks.pick_date == d_date.date
            group by 1, 2   
        ),
        order_stats_for_agg_period as (
            select
                orders.{z_score_group},
                coalesce(avg(orders.pick_volume), 0) as mean_pick_volume,
                coalesce(stddev(orders.pick_volume), 1) as std_pick_volume
            from orders
            group by 1
        ),
        z_score_agg as (
            select
                orders.*,
                ((orders.pick_volume - order_stats_for_agg_period.mean_pick_volume) / order_stats_for_agg_period.std_pick_volume) as zscore
            from orders
            left join order_stats_for_agg_period
            on orders.{z_score_group} == order_stats_for_agg_period.{z_score_group}
        )
        select * from z_score_agg order by {z_score_group};
        """).df()

    # Ensure the output directory exists
    weekly_zscore_distribution_path = path.join(data_mart_path, "weekly_zscore_distribution")
    if not path.exists(weekly_zscore_distribution_path):
        os.mkdir(weekly_zscore_distribution_path)
    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(data_mart_path, DataMartNames.weekly_zscore_distribution))
    logger.info("Processed function: weekly_zscore_distribution")
    outliers = agg_df[agg_df['zscore'].abs() > 3]
    number_of_outliers = len(outliers)
    logger.info(f"Total Outliers Identified: {number_of_outliers}")
    return agg_df


def order_mix(f_order_picks: pd.DataFrame) -> pd.DataFrame:
    """
     Calculates the percentage contribution of each warehouse section to the total pick volume
     for every order. Saves the results as a Parquet file for analysis.

     Args:
         f_order_picks (pd.DataFrame): DataFrame containing curated order pick data.

     Returns:
         None: Writes the aggregated data to a Parquet file.
     """
    logger.info("Starting to process function: order_mix")
    global data_mart_path

    # SQL query to calculate order mix
    logger.debug("Executing SQL query to calculate the percentage contribution of warehouse sections to order volume.")
    agg_df = duckdb.sql("""

               WITH pick_vol_per_order_per_section AS(
                select 
                    sk_order_id,
                    warehouse_section,
                    sum(pick_volume) as sum_pick_volume,
                from
                    f_order_picks
                group by 1,2
                ),
                pick_vol_per_order AS(
                select
                    sk_order_id,
                    min(pick_date) as order_date,
                    sum(pick_volume) as sum_pick_volume
                from
                    f_order_picks
                group by 1
                )
                select 
                po.order_date,
                ps.sk_order_id,
                ps.warehouse_section,
                round(ps.sum_pick_volume/po.sum_pick_volume, 4)* 100 as section_pick_percentage
                from 
                    pick_vol_per_order_per_section as ps
                left join 
                     pick_vol_per_order as po
                on ps.sk_order_id = po.sk_order_id
                order by ps.sk_order_id


    """).df()
    # Ensure the output directory exists
    order_mix_path = path.join(data_mart_path, "order_mix")
    if not path.exists(order_mix_path):
        os.mkdir(order_mix_path)
    # Save the aggregated data as a Parquet file
    agg_df.to_parquet(path.join(order_mix_path, "order_mix.parquet"))
    return agg_df


if __name__ == "__main__":
    logger.info("Reading data: f_order_picks")
    f_order_picks = pd.read_parquet(path.join(curation_path, CurationFileNames.f_order_picks))
    logger.info("Reading data: d_date")
    d_date = pd.read_parquet(path.join(curation_path, CurationFileNames.d_date))
    logger.info("Reading data: d_product_details")
    d_product_details = pd.read_parquet(path.join(curation_path, CurationFileNames.d_product_details))
    logger.info("Enriching data: f_order_picks")
    f_order_picks = f_order_picks.merge(d_product_details, on="product_id", how="left")
    logger.info("Reading data: d_warehouse_section")
    d_warehouse_section = pd.read_parquet(path.join(curation_path, CurationFileNames.d_warehouse_section))
    logger.info("Reading data: f_returns")
    f_returns = pd.read_parquet(path.join(curation_path, CurationFileNames.f_returns))
    logger.info("Reading data: f_pick_errors")
    f_pick_errors = pd.read_parquet(path.join(curation_path, CurationFileNames.f_pick_errors))
    logger.info("Enriching data: f_pick_errors")
    f_pick_errors = f_pick_errors.merge(d_product_details, on="product_id", how="left")

    logger.info("Starting Transformations")
    total_pick_volume(f_order_picks=f_order_picks, d_date=d_date)
    total_orders_processed(d_date=d_date, f_order_picks=f_order_picks)
    pick_errors(f_order_picks=f_order_picks, d_date=d_date, f_pick_errors=f_pick_errors)
    top_n_products_weekly(f_order_picks=f_order_picks, d_date=d_date)
    avg_products_picked_per_order(f_order_picks=f_order_picks, d_date=d_date)
    order_count_by_type(f_order_picks=f_order_picks, d_date=d_date)
    warehouse_utilization_per_section(f_order_picks=f_order_picks, d_date=d_date)
    pick_throughput(f_order_picks=f_order_picks, d_date=d_date)
    binned_order_volume(f_order_picks=f_order_picks, d_date=d_date)
    weekly_zscore_distribution(f_order_picks=f_order_picks, d_date=d_date)
    order_mix(f_order_picks=f_order_picks)
    for drill_down in common_drill_downs:
        total_pick_volume_w_drill_down(f_order_picks=f_order_picks, d_date=d_date, drill_down=drill_down)
        total_orders_processed_w_drill_down(d_date=d_date, f_order_picks=f_order_picks, drill_down=drill_down)
        pick_errors_w_drill_down(f_order_picks=f_order_picks, d_date=d_date, f_pick_errors=f_pick_errors,
                                 drill_down=drill_down)
        top_n_products_weekly_w_drill_down(f_order_picks=f_order_picks, d_date=d_date, drill_down=drill_down)
        pick_throughput_w_drill_down(f_order_picks=f_order_picks, d_date=d_date, drill_down=drill_down)
    logger.info("\n\nCompleted all transformations!")
