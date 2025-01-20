import pandas as pd
from enums import DataMartNames, CurationFileNames
from utils import store_df_as_hyper
from os import path, mkdir
from constants import BASE_PATH
import duckdb

curation_path = path.join(BASE_PATH, "curation")
if not path.exists(curation_path):
    raise FileNotFoundError(
        f"Curation data cannot be found. Expected curation data at location {BASE_PATH}/curation"
    )
data_mart_path = path.join(BASE_PATH, "data_mart")
if not path.exists(data_mart_path):
    mkdir(data_mart_path)


def total_pick_volume(pick_df: pd.DataFrame, d_date: pd.DataFrame):
    global data_mart_path

    agg_df = duckdb.sql(
        """
            WITH agg_cte AS (
                SELECT 
                    pick_date,
                    sum(pick_volume) AS pick_volume
                FROM
                    pick_df
                GROUP BY pick_df.pick_date
            )
            SELECT
                d_date.date,
                coalesce(agg_cte.pick_volume, 0) as pick_volume,
                d_date.week,
                d_date.month,
                d_date.quarter,
                d_date.year_half
            FROM
                d_date
            LEFT JOIN
                agg_cte
            ON
                d_date.date == agg_cte.pick_date
            ORDER BY d_date.date
        """
    ).df()
    agg_df.to_parquet(path.join(data_mart_path, DataMartNames.total_pick_volume))
    store_df_as_hyper(
        df=agg_df,
        table_name=DataMartNames.total_pick_volume,
        location=data_mart_path,
    )


def pick_volume_per_product_group(
    d_date: pd.DataFrame,
    pick_df: pd.DataFrame,
    product_df: pd.DataFrame,
):
    global data_mart_path
    agg_df = duckdb.sql(
        """
            WITH agg_cte AS (
                SELECT
                    pick_df.pick_date,
                    product_df.product_group,
                    sum(pick_volume) AS pick_volume
                FROM
                    pick_df
                LEFT JOIN
                    product_df
                ON
                    pick_df.product_id == product_df.product_id
                GROUP BY
                    pick_df.pick_date,
                    product_df.product_group
            )
            SELECT
                d_date.date,
                product_df.product_group,
                coalesce(agg_cte.pick_volume, 0) AS pick_volume,
                d_date.week,
                d_date.month,
                d_date.quarter,
                d_date.year_half
            FROM
                d_date
            CROSS JOIN (SELECT DISTINCT(product_group) FROM product_df) AS product_df
            LEFT JOIN
                agg_cte
            ON
                d_date.date == agg_cte.pick_date
                AND product_df.product_group = agg_cte.product_group
            ORDER BY d_date.date
            
        """
    ).df()

    agg_df.to_parquet(
        path.join(data_mart_path, DataMartNames.pick_volume_per_product_group)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=DataMartNames.pick_volume_per_product_group,
        location=data_mart_path,
    )


def order_volume(pick_df: pd.DataFrame, d_date: pd.DataFrame):
    global data_mart_path

    agg_df = duckdb.sql(
        """
            WITH agg_cte AS (
                SELECT 
                    pick_date,
                    COUNT(DISTINCT(sk_order_id)) AS order_volume
                FROM
                    pick_df
                GROUP BY pick_df.pick_date
            )
            SELECT
                d_date.date,
                coalesce(agg_cte.order_volume, 0) as order_volume,
                d_date.week,
                d_date.month,
                d_date.quarter,
                d_date.year_half
            FROM
                d_date
            LEFT JOIN
                agg_cte
            ON
                d_date.date == agg_cte.pick_date
            ORDER BY d_date.date

        """
    ).df()
    agg_df.to_parquet(path.join(data_mart_path, DataMartNames.order_volume))
    store_df_as_hyper(
        df=agg_df,
        table_name=DataMartNames.order_volume,
        location=data_mart_path,
    )


def binned_order_volume(pick_df: pd.DataFrame, d_date: pd.DataFrame):
    raw_order_df = duckdb.sql(
        """
        select
            pick_date,
            sk_order_id,
            sum(pick_volume) as pick_volume,
        from pick_df
        group by 1, 2
        """
    ).df()
    bins = [0, 50, 150, 350, 600, 900, 200000]
    labels = ["mini", "small", "medium", "large", "extra_large", "extreme"]
    labels_df = pd.DataFrame({"bins": labels})
    raw_order_df["bins"] = pd.cut(raw_order_df["pick_volume"], bins=bins, labels=labels)
    time_series_df = duckdb.sql(
        """
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
        """
    ).df()
    time_series_df.rename({"bins": "category"})
    time_series_df.to_parquet(
        path.join(data_mart_path, DataMartNames.binned_order_volume)
    )
    store_df_as_hyper(
        df=time_series_df,
        table_name=DataMartNames.binned_order_volume,
        location=data_mart_path,
    )


def weekly_zscore_distribution(
    pick_df: pd.DataFrame, d_date: pd.DataFrame, z_score_group: str = "week"
):
    agg_df = duckdb.sql(
        f"""
        with orders as (
            select
                pick_df.sk_order_id,
                d_date.{z_score_group},
                sum(pick_df.pick_volume) as pick_volume
            from pick_df
            left join d_date
                on pick_df.pick_date == d_date.date
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
        """
    ).df()
    agg_df.to_parquet(
        path.join(data_mart_path, DataMartNames.weekly_zscore_distribution)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=DataMartNames.weekly_zscore_distribution,
        location=data_mart_path,
    )


def error_rate_per_warehouse_section(
    pick_df: pd.DataFrame,
    error_df: pd.DataFrame,
    d_date: pd.DataFrame,
    d_warehouse_section: pd.DataFrame,
):
    agg_df = duckdb.sql(
        """
        with date_warehouse_cross as (
            select
                *
            from d_date
            cross join 
                (select distinct (pick_reference) as warehouse_section from d_warehouse_section where pick_reference != '(nicht vorhanden)')
        ),
        errors as (
            select
                date_warehouse_cross.date,
                date_warehouse_cross.warehouse_section,
                count(*) as error_count                
            from date_warehouse_cross
            left join
                error_df
            on date_warehouse_cross.date = error_df.pick_date
            group by 1, 2
        ),
        picks as (
            select
                date_warehouse_cross.date,
                date_warehouse_cross.warehouse_section,
                count(*) as total_picks                
            from date_warehouse_cross
            left join
                pick_df
            on date_warehouse_cross.date = pick_df.pick_date
            group by 1, 2
        ),
        result as (
            select
                errors.date,
                errors.warehouse_section,
                errors.error_count,
                picks.total_picks,
                d_date.week,
                d_date.month,
                d_date.quarter,
                d_date.year_half,
                d_date.year
            from errors
            left join picks
            on 
                errors.date = picks.date
                and errors.warehouse_section = picks.warehouse_section
            left join d_date
            on 
                errors.date = d_date.date
        )
        select * from result
        """
    ).df()
    agg_df.to_parquet(
        path.join(data_mart_path, DataMartNames.error_rate_per_warehouse_section)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=DataMartNames.error_rate_per_warehouse_section,
        location=data_mart_path,
    )


def template_function(
        d_date: pd.DataFrame,
        f_pick_errors: pd.DataFrame,
):
    """

    """
    datamart_name = ""
    datamart_name = datamart_name + ".parquet"
    sql_script = """
    """
    agg_df = duckdb.sql(sql_script).df()
    agg_df.to_parquet(
        path.join(data_mart_path, datamart_name)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=datamart_name,
        location=data_mart_path,
    )

def order_mix(
        d_date: pd.DataFrame,
        f_pick_errors: pd.DataFrame,
):
    """

    """
    datamart_name = "order_mix"
    datamart_name = datamart_name + ".parquet"
    sql_script = """
    select
        count(order_number),
        origin
    f_order_picks
    """
    agg_df = duckdb.sql(sql_script).df()
    agg_df.to_parquet(
        path.join(data_mart_path, datamart_name)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=datamart_name,
        location=data_mart_path,
    )

def total_orders_processed(
        d_date: pd.DataFrame,
        f_order_picks: pd.DataFrame,
):
    """

    """
    datamart_name = "total_orders_processed"
    datamart_name = datamart_name + ".parquet"
    sql_script = """
    SELECT 
        COUNT(DISTINCT order_number) AS total_orders_processed,
        d_date.date,d_date.week
    FROM f_order_picks
    LEFT JOIN d_date
    ON f_order_picks.pick_date = d_date.date
    GROUP BY d_date.week ,d_date.date
    """
    agg_df = duckdb.sql(sql_script).df()
    agg_df.to_parquet(
        path.join(data_mart_path, datamart_name)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=datamart_name,
        location=data_mart_path,
    )

def top_10_products(
        d_date: pd.DataFrame,
        f_order_picks: pd.DataFrame,
):
    """

    """
    datamart_name = "top_10_products"
    datamart_name = datamart_name + ".parquet"
    sql_script = """
 SELECT
    f.product_id,
    sum(pick_volume   ) * 100/ (SELECT COUNT(*) FROM f_order_picks) AS top_products,
    d.week
FROM f_order_picks AS f
LEFT JOIN d_date AS d
    ON f.pick_date = d.date
GROUP BY
    f.product_id,
    d.week
ORDER BY
    top_products DESC
    """
    agg_df = duckdb.sql(sql_script).df()
    agg_df.to_parquet(
        path.join(data_mart_path, datamart_name)
    )
    store_df_as_hyper(
        df=agg_df,
        table_name=datamart_name,
        location=data_mart_path,
    )

# def warehouse_utilization_per_section(
#         d_date: pd.DataFrame,
#         f_order_picks: pd.DataFrame,
# ):
#     """
#
#     """
#     datamart_name = "warehouse_utilization_per_section"
#     datamart_name = datamart_name + ".parquet"
#     sql_script = """
#
#
#     """
#     agg_df = duckdb.sql(sql_script).df()
#     agg_df.to_parquet(
#         path.join(data_mart_path, datamart_name)
#     )
#     store_df_as_hyper(
#         df=agg_df,
#         table_name=datamart_name,
#         location=data_mart_path,
#     )

#
# def pick_error_per_warehouse_sections(
#         d_date: pd.DataFrame,
#         f_pick_errors: pd.DataFrame,
#         f_order_picks: pd.DataFrame,
# ):
#     """
#     week
#     month
#     warehouse_section
#     total_errors
#     total_picks
#     """
#     datamart_name = "pick_error_per_warehouse_sections"
#     datamart_name = datamart_name + ".parquet"
#     sql_script = """
#     with total_errors_cte as (
#         select
#             d_date.week,
#             d_date.month,
#             pe.warehouse_section,
#             count(*) as total_errors
#         from f_pick_errors as pe
#         left join d_date
#             on pe.pick_date = d_date.date
#         group by 1, 2, 3
#     ),
#     total_picks_cte as (
#         select
#             d_date.week,
#             d_date.month,
#             op.warehouse_section,
#             count(*) as total_picks
#         from f_order_picks as op
#         left join d_date
#             on op.pick_date = d_date.date
#         group by 1, 2, 3
#     )
#     select
#         total_errors_cte.*,
#         total_picks_cte.total_picks
#     from total_errors_cte
#     left join total_picks_cte
#     on total_errors_cte.week = total_picks_cte.week
#     and total_errors_cte.month = total_picks_cte.month
#     and total_errors_cte.warehouse_section = total_picks_cte.warehouse_section;
#
#     """
#     agg_df = duckdb.sql(sql_script).df()
#     agg_df.to_parquet(
#         path.join(data_mart_path, datamart_name)
#     )
#     store_df_as_hyper(
#         df=agg_df,
#         table_name=datamart_name,
#         location=data_mart_path,
#     )

if __name__ == "__main__":
    # Read Curation Data
    f_order_picks = pd.read_parquet(
        path.join(curation_path, CurationFileNames.f_order_picks)
    )
    d_date = pd.read_parquet(path.join(curation_path, CurationFileNames.d_date))
    d_product_details = pd.read_parquet(
        path.join(curation_path, CurationFileNames.d_product_details)
    )
    d_warehouse_section = pd.read_parquet(
        path.join(curation_path, CurationFileNames.d_warehouse_section)
    )
    f_returns = pd.read_parquet(path.join(curation_path, CurationFileNames.f_returns))
    f_pick_errors = pd.read_parquet(
        path.join(curation_path, CurationFileNames.f_pick_errors)
    )

    # Execute Data Mart Transformations
    # total_pick_volume(pick_df=f_order_picks, d_date=d_date)
    # pick_volume_per_product_group(
    #     d_date=d_date, pick_df=f_order_picks, product_df=d_product_details
    # )
    # warehouse_utilization_per_section(f_order_picks=f_order_picks, d_date=d_date)
    # d_warehouse_utilization_per_section = pd.read_parquet(
    #     path.join(curation_path, CurationFileNames.warehouse_utilization_per_section)
    # )

    # pick_error_per_warehouse_sections(d_date=d_date, f_pick_errors=f_pick_errors, f_order_picks=f_order_picks)
    # d_pick_error_per_warehouse_sections = pd.read_parquet(
    #     path.join(curation_path, CurationFileNames.pick_error_per_warehouse_sections)
    # )
    # total_orders_processed( f_order_picks=f_order_picks, d_date=d_date)
    # total_orders_processed = pd.read_parquet(
    #         path.join(data_mart_path, "total_orders_processed.parquet")
    #     )
    top_10_products(f_order_picks=f_order_picks, d_date=d_date)
    top_10_products = pd.read_parquet(
            path.join(data_mart_path, "top_10_products.parquet")
        )
    # order_volume(pick_df=f_order_picks, d_date=d_date)
    # binned_order_volume(pick_df=f_order_picks, d_date=d_date)
    # weekly_zscore_distribution(pick_df=f_order_picks, d_date=d_date)
    # error_rate_per_warehouse_section(
    #     f_order_picks,
    #     f_pick_errors,
    #     d_date,
    #     d_warehouse_section,
    # )
