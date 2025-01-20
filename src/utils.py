from os import path
import pandas as pd
import pantab


def store_df_as_hyper(df: pd.DataFrame, table_name: str, location: str):
    if table_name.endswith(".parquet"):
        table_name = path.splitext(table_name)[0]
    pantab.frame_to_hyper(df, location + f"/{table_name}.hyper", table=table_name)
