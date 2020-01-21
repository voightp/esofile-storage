import pandas as pd
import time


def profile(func):
    def inner_func(*args, **kwargs):
        s = time.perf_counter()
        res = func(*args, **kwargs)
        e = time.perf_counter()
        print(f"Func: {func.__name__} - time: '{e - s}'s")
        return res

    return inner_func


@profile
def merge_df_values(df: pd.DataFrame, separator: str = " ") -> pd.Series:
    """ Merge all column values into a single str pd.Series. """
    df = df.astype(str)
    str_df = df.apply(lambda x: f"{separator}".join(x.to_list()))
    return str_df


@profile
def split_stuff(df, separator):
    # transform string fields into list
    return df.applymap(lambda x: x.split(separator))


@profile
def dataframe_stuff(df):
    sr = df.iloc[0, :]
    dct = {}
    for index, val in sr.iteritems():
        dct[index] = val
    df = pd.DataFrame(dct, dtype=float)

    return df


@profile
def destringify_df(df: pd.DataFrame, separator="\t"):
    """ Transform joined str field into numeric columns. """
    df = split_stuff(df, separator=separator).T
    df = dataframe_stuff(df)
    return df
