import os
from os import path, mkdir

import duckdb
import pandas as pd

from constants import BASE_PATH
from enums import DataMartNames, CurationFileNames

curation_path = path.join(BASE_PATH, "curation")
if not path.exists(curation_path):
    raise FileNotFoundError(f"Curation data cannot be found. Expected curation data at location {BASE_PATH}/curation")
data_mart_path = path.join(BASE_PATH, "data_mart")
if not path.exists(data_mart_path):
    mkdir(data_mart_path)

common_drill_downs = ["product_group", "origin", "warehouse_section"]


def total_pick_volume(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: total_pick_volume")
    global data_mart_path
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
    total_pick_volume_path = path.join(data_mart_path, "total_pick_volume")
    if not path.exists(total_pick_volume_path):
        os.mkdir(total_pick_volume_path)
    agg_df.to_parquet(path.join(total_pick_volume_path, "total_pick_volume.parquet"))
    for drill_down in common_drill_downs:
        print(f"Currently processing drill down: {drill_down}")
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
        agg_df.to_parquet(path.join(total_pick_volume_path, f"total_pick_volume_{drill_down}.parquet"))
        print(f"Done processing drill down: {drill_down}")
    print("Processed function: total_pick_volume")


def total_orders_processed(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: total_orders_processed")
    global data_mart_path
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
    total_orders_processed_path = path.join(data_mart_path, "total_orders_processed")
    if not path.exists(total_orders_processed_path):
        os.mkdir(total_orders_processed_path)
    agg_df.to_parquet(path.join(total_orders_processed_path, "total_orders_processed.parquet"))
    for drill_down in common_drill_downs:
        print(f"Currently processing drill down: {drill_down}")
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
        agg_df = duckdb.sql(sql_script).df()
        agg_df.to_parquet(path.join(total_orders_processed_path, f"total_orders_processed_{drill_down}.parquet"))
        print(f"Done processing drill down: {drill_down}")
    print("Processed function: total_orders_processed")


def pick_errors(f_order_picks: pd.DataFrame, f_pick_errors: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: pick_errors")
    global data_mart_path
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
    pick_errors_path = path.join(data_mart_path, "pick_errors")
    if not path.exists(pick_errors_path):
        os.mkdir(pick_errors_path)
    agg_df.to_parquet(path.join(pick_errors_path, "pick_errors.parquet"))

    for drill_down in common_drill_downs:
        print(f"Currently processing drill down: {drill_down}")
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
        agg_df = duckdb.sql(sql_script).df()
        agg_df.to_parquet(path.join(pick_errors_path, f"pick_errors_{drill_down}.parquet"))
        print(f"Done processing drill down: {drill_down}")
    print("Processed function: pick_errors")


def top_n_products_weekly(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: top_n_products_weekly")
    global data_mart_path
    n = 10
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
    top_n_products_weekly_path = path.join(data_mart_path, "top_n_products_weekly")
    if not path.exists(top_n_products_weekly_path):
        os.mkdir(top_n_products_weekly_path)
    agg_df.to_parquet(path.join(top_n_products_weekly_path, "top_n_products_weekly.parquet"))

    for drill_down in common_drill_downs:
        print(f"Currently processing drill down: {drill_down}")
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
        agg_df.to_parquet(path.join(top_n_products_weekly_path, f"top_n_products_weekly_{drill_down}.parquet"))
        print(f"Done processing drill down: {drill_down}")
    print("Processed function: top_n_products_weekly")


def avg_products_picked_per_order(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: avg_products_picked_per_order")
    global data_mart_path
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
    avg_products_picked_per_order_path = path.join(data_mart_path, "avg_products_picked_per_order")
    if not path.exists(avg_products_picked_per_order_path):
        os.mkdir(avg_products_picked_per_order_path)
    agg_df.to_parquet(path.join(avg_products_picked_per_order_path, "avg_products_picked_per_order.parquet"))
    print("Processed function: avg_products_picked_per_order")


def order_mix_per_origin(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: order_mix_per_origin")
    global data_mart_path
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
    order_mix_per_origin_path = path.join(data_mart_path, "order_mix_per_origin")
    if not path.exists(order_mix_per_origin_path):
        os.mkdir(order_mix_per_origin_path)
    agg_df.to_parquet(path.join(order_mix_per_origin_path, "order_mix_per_origin.parquet"))
    print("Processed function: order_mix_per_origin")


def warehouse_utilization_per_section(d_date: pd.DataFrame, f_order_picks: pd.DataFrame, ):
    print("-" * 10)
    print("Starting to process function: warehouse_utilization_per_section")
    global data_mart_path
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
    warehouse_utilization_per_section_path = path.join(data_mart_path, "warehouse_utilization_per_section")
    if not path.exists(warehouse_utilization_per_section_path):
        os.mkdir(warehouse_utilization_per_section_path)
    agg_df.to_parquet(path.join(warehouse_utilization_per_section_path, "warehouse_utilization_per_section.parquet"))
    print("Processed function: pick_throughput")


def pick_throughput(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("-" * 10)
    print("-" * 10)
    print("Starting to process function: pick_throughput")
    global data_mart_path
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
                    round(avg(pick_volume), 2) as weekly_pick_throughput_avg
                from hourly_agg_cte
                left join d_date
                    on hourly_agg_cte.pick_date = d_date.date
                group by 1
            )
            select * from weekly_avg
        """).df()
    pick_throughput_path = path.join(data_mart_path, "pick_throughput")
    if not path.exists(pick_throughput_path):
        os.mkdir(pick_throughput_path)
    agg_df.to_parquet(path.join(pick_throughput_path, "pick_throughput.parquet"))
    print("Processed function: pick_throughput without individual. Starting drill downs")
    for drill_down in common_drill_downs:
        print(f"Currently processing drill down: {drill_down}")
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
        agg_df = duckdb.sql(sql_script).df()
        agg_df.to_parquet(path.join(pick_throughput_path, f"pick_throughput_{drill_down}.parquet"))
        print(f"Done processing drill down: {drill_down}")
    print("Processed function: pick_throughput")

def binned_order_volume(f_order_picks: pd.DataFrame, d_date: pd.DataFrame):
    print("-" * 10)
    print("Starting to process function: binned_order_volume")
    raw_order_df = duckdb.sql("""
        select
            pick_date,
            sk_order_id,
            sum(pick_volume) as pick_volume,
        from f_order_picks
        group by 1, 2
        """).df()
    bins = [0, 50, 150, 350, 600, 900, 200000]
    labels = ["mini", "small", "medium", "large", "extra_large", "extreme"]
    labels_df = pd.DataFrame({"bins": labels})
    raw_order_df["bins"] = pd.cut(raw_order_df["pick_volume"], bins=bins, labels=labels)
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
    time_series_df.rename({"bins": "category"})
    time_series_df.to_parquet(path.join(data_mart_path, DataMartNames.binned_order_volume))
    print("Processed function: binned_order_volume")


def weekly_zscore_distribution(f_order_picks: pd.DataFrame, d_date: pd.DataFrame, z_score_group: str = "week"):
    print("-" * 10)
    print("Starting to process function: weekly_zscore_distribution")
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
    agg_df.to_parquet(path.join(data_mart_path, DataMartNames.weekly_zscore_distribution))
    print("Processed function: weekly_zscore_distribution")

if __name__ == "__main__":
    print("Reading data: f_order_picks")
    f_order_picks = pd.read_parquet(path.join(curation_path, CurationFileNames.f_order_picks))
    print("Reading data: d_date")
    d_date = pd.read_parquet(path.join(curation_path, CurationFileNames.d_date))
    print("Reading data: d_product_details")
    d_product_details = pd.read_parquet(path.join(curation_path, CurationFileNames.d_product_details))
    print("Enriching data: f_order_picks")
    f_order_picks = f_order_picks.merge(d_product_details, on="product_id", how="left")
    print("Reading data: d_warehouse_section")
    d_warehouse_section = pd.read_parquet(path.join(curation_path, CurationFileNames.d_warehouse_section))
    print("Reading data: f_returns")
    f_returns = pd.read_parquet(path.join(curation_path, CurationFileNames.f_returns))
    print("Reading data: f_pick_errors")
    f_pick_errors = pd.read_parquet(path.join(curation_path, CurationFileNames.f_pick_errors))
    print("Enriching data: f_pick_errors")
    f_pick_errors = f_pick_errors.merge(d_product_details, on="product_id", how="left")

    print("Starting Transformations")
    total_pick_volume(f_order_picks=f_order_picks, d_date=d_date)
    total_orders_processed(d_date=d_date, f_order_picks=f_order_picks)
    pick_errors(f_order_picks=f_order_picks, d_date=d_date, f_pick_errors = f_pick_errors )
    top_n_products_weekly(f_order_picks=f_order_picks, d_date=d_date)
    avg_products_picked_per_order(f_order_picks=f_order_picks, d_date=d_date)
    order_mix_per_origin(f_order_picks=f_order_picks, d_date=d_date)
    warehouse_utilization_per_section(f_order_picks=f_order_picks, d_date=d_date)
    pick_throughput(f_order_picks=f_order_picks, d_date=d_date)
    binned_order_volume(f_order_picks=f_order_picks, d_date=d_date)
    weekly_zscore_distribution(f_order_picks=f_order_picks, d_date=d_date)
    print("\n\nCompleted all transformations!")
  