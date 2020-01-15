import pandas as pd
from typing import List


def merge_df_values(df: pd.DataFrame, separator: str = " ") -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df


def destringify_df(df: pd.DataFrame, separator="\t"):
    """ Transform joined str field into numeric columns. """
    # transform string fields into list
    df = df.applymap(lambda x: x.split(separator))

    # convert cell lists into rows
    df = df["values"].apply(pd.Series)

    return df
