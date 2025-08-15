from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, cast

import pandas as pd

from mathematics import clamp

from . import SequenceType, timeutil

AxisType = Literal["index", "columns"]


def search_timeidx(
    time: datetime, df: pd.DataFrame, indexname: str, isclamp: bool = False
):
    """
    在 DataFrame 中搜索特定时间的索引位置。

    本函数旨在查找给定时间在 DataFrame 中的插入位置，可以选择性地对结果索引进行边界检查以防止越界。
    查找插入位置
    new <= old
    参数:
    - time: datetime 类型，指定要搜索的时间。
    - df: pd.DataFrame 类型，包含时间相关索引或列的数据表。
    - indexname: 字符串类型，指定 DataFrame 中的时间相关索引或列名，默认为 "date"。
    - isclamp: 布尔类型，指示是否对结果索引进行边界检查，默认为 False。

    返回:
    - int 类型，时间在 DataFrame 中的插入位置索引。
    """
    time = timeutil.to_utctz(time=time)

    if indexname == df.index.name:
        column = df.index
    elif indexname in df.columns:
        column = df[indexname]
    else:
        raise ValueError("Invalid indexname: indexname must be in df.columns or index.")
    column = timeutil.to_utctz(column)
    idx = cast(int, column.searchsorted(value=time, side="left"))
    idx = clamp(x=idx, min_v=0, max_v=len(df) - 1) if isclamp else idx
    return int(idx)


def shift(
    df: pd.DataFrame,
    names: SequenceType,
    periods: int = 1,
    axis: str = "columns",
    must_include_names: bool = True,
):
    """
    {'A': [1, 2, 3, 4, 5],
    'B': [10, 20, 30, 40, 50]}

    df_shifted = df.shift(periods=1)

        A     B
    0  NaN   NaN
    1  1.0  10.0
    2  2.0  20.0
    3  3.0  30.0
    4  4.0  40.0
    """
    if periods == 0 or len(names) <= 0:
        return df.copy()
    if axis == "columns":
        included = [n for n in names if n in df.columns]
        loc_key = (slice(None), [name for name in names])
        shift_axis = 0
    elif axis == "index":
        included = [n for n in names if n in df.index]
        loc_key = (included, slice(None))
        shift_axis = 1
    else:
        raise ValueError("axis must be 'columns' or 'index'")

    if must_include_names:
        if len(names) > len(included):
            raise ValueError(
                f"names must be in df: {list(set(names) - set(included))  } are not in df"
            )
        if len(included) <= 0:
            raise ValueError("names must be in df! No valid names found.")
    elif len(included) <= 0:
        return df.copy()
    df_copy = df.copy()
    df_copy.loc[loc_key] = df_copy.loc[loc_key].shift(periods=periods, axis=shift_axis)
    return df_copy


def is_default_index(df: pd.DataFrame) -> bool:
    if isinstance(df.index, pd.RangeIndex):
        return df.index.equals(pd.RangeIndex(len(df)))
    return False


def rename(df: pd.DataFrame, columns: dict):
    """
    Rename columns in a DataFrame, including the index if it needs to be renamed.
    \n
    Parameters:
    \n
      columns (dict)\n
    {\n
      old column name: new column name
    }\n


    ***None. The renaming is done in place.
    """
    oldindexname = None
    if df.index.name in columns.keys():
        oldindexname = df.index.name
        df.index.rename(columns[oldindexname], inplace=True)
        columns.pop(oldindexname)
    df.rename(columns=columns, inplace=True)


def setindex(df: pd.DataFrame, key: str):

    if key in df.columns:
        df.set_index(key, inplace=True, drop=True)
        return True
    elif key == df.index.name:
        return True
    else:
        return False


def set_timeidx(df: pd.DataFrame, key: str, timezone=None):
    suscu = setindex(df, key)
    df.index = timeutil.to_tz(df.index, timezone)
    return suscu


method = Literal["max", "min", "mean", "sum", "first", "last"]


def combinefirst_bytime(
    key: str, df1: pd.DataFrame, df2: pd.DataFrame, reset_index: bool = True
):
    susc1 = set_timeidx(df1, key, timezone.utc)
    susc2 = set_timeidx(df2, key, timezone.utc)
    suc = susc1 and susc2
    df = pd.DataFrame() if not suc else df1.combine_first(df2)
    if susc1:
        df1.reset_index(inplace=True, drop=False)
    if susc2:
        df2.reset_index(inplace=True, drop=False)
    if not reset_index:
        df.reset_index(inplace=True, drop=False)
    return df


def resample(
    data_df: pd.DataFrame,
    freq: str,
    *m: tuple[
        Sequence[str],
        method | Callable[[pd.DataFrame | pd.Series], pd.DataFrame | pd.Series],
    ],
):
    df = pd.DataFrame(columns=data_df.columns)
    dfs = data_df.resample(rule=freq)
    for k, delegate in m:
        if isinstance(delegate, Callable):
            df[k] = dfs[k].apply(delegate)
        elif isinstance(delegate, str):
            match delegate:
                case "first":
                    df[k] = dfs[k].first()
                case "last":
                    df[k] = dfs[k].last()
                case "mean":
                    df[k] = dfs[k].mean()
                case "max":
                    df[k] = dfs[k].max()
                case "min":
                    df[k] = dfs[k].min()
                case "sum":
                    df[k] = dfs[k].sum()
    return df


def readpd(p: Path | str):
    if isinstance(p, str):
        p = Path(p)
    suffix = p.suffix.lower()
    match suffix:
        case ".csv":
            return pd.read_csv(p)
        case ".feather":
            return pd.read_feather(p)
        case ".parquet":
            return pd.read_parquet(p)
        case _:
            raise TypeError("file format not support ", suffix, " : ", p)


def writepd(df: pd.DataFrame, p: Path | str, index: bool | None = None):
    if isinstance(p, str):
        p = Path(p)

    suffix = p.suffix.lower()
    match suffix:
        case ".csv":
            if index is None:
                index = not is_default_index(df)
            if df.index.name is None:
                return df.to_csv(path_or_buf=p, index=index)
            else:
                return df.to_csv(path_or_buf=p, index=index, index_label=df.index.name)

        case ".feather":
            return df.to_feather(path=p)
        case ".parquet":
            return df.to_parquet(path=p, index=index)
        case _:
            raise TypeError("file format not support ", suffix, " : ", p)


def sum_none(df: pd.DataFrame, axis: AxisType = "columns"):
    """
    Calculate the number of missing (NaN) values in each column of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to check for missing values.

    Returns:
        dict: A dictionary where the keys are column names and the values are the counts of missing values in each column.
    """

    if df.empty:
        return {}

    if axis == "index":
        na_counts = df.isna().sum(axis=1)
        result = {index: count for index, count in na_counts.items() if count > 0}
    elif axis == "columns":
        na_counts = df.isna().sum(axis=0)
        result = {column: count for column, count in na_counts.items() if count > 0}

    return result
