import calendar
import re
from datetime import datetime, timedelta, timezone, tzinfo
from typing import overload

import pandas as pd
from numpy import datetime64, timedelta64

from . import TimeFormat
from .type import DatetimeType, TimedeltaType


def now(tz=timezone.utc):
    """获取当前utc时间"""

    return datetime.now(tz=tz)


def strNow(format: str, tz=timezone.utc):
    """
    format
    %y     两位数年份 00-99
    %Y     四位数年份表示 000-9999
    %m     月份 01-12
    %d     月内中的一天0-31
    %H     24小时制小时数 0-23
    %I     12小时制小时数 01-12
    %M     分钟数 00-59
    %S     秒 00-59
    %a     本地简化星期名称
    %A     本地完整星期名称
    %b     本地简化的月份名称
    %B     本地完整的月份名称
    %c     本地相应的日期表示和时间表示
    %j     年内的一天 001-366
    %p     本地A.M.或P.M.的等价符
    %U     一年中的星期数 00-53星期天开始
    %w     星期0-6 星期天为星期的开始
    %W     一年中的星期数 00-53 星期一开始
    %x     本地相应的日期
    %X     本地相应的时间
    %Z     当前时区的名称
    %%     %号本身

    """
    return now(tz=tz).strftime(format)


def timedelta2s(seconds: TimedeltaType):
    if isinstance(seconds, timedelta):
        return seconds.total_seconds()
    elif isinstance(seconds, pd.Timedelta):
        return seconds.total_seconds()
    elif isinstance(seconds, timedelta64):
        return seconds / timedelta64(1, "s")
    else:
        raise TypeError(seconds)


# 定义函数将总秒数转换为频率字符串
def timedelta2freq(delta: TimedeltaType, secondsunit: dict[int, str]):
    seconds = timedelta2s(delta)
    result = ""
    for sec, unit in secondsunit.items():
        value = int(seconds // sec)
        if value > 0:
            result += f"{int(value)}{unit}"
            seconds %= sec
    return result


def datetime2ms(time: DatetimeType) -> int:
    """
    将给定的时间转换为自1970年1月1日以来的毫秒数。

    参数:
    time: datetimeUnion类型，可以是numpy的datetime64对象或其它可转换为datetime64的类型。

    返回:
    整数，表示自1970年1月1日以来的毫秒数。
    """
    if isinstance(time, datetime64):
        return int((time - datetime64("1970-01-01T00:00:00Z")) / timedelta64(1, "ms"))

    return int(pd.to_datetime(time).timestamp() * 1e3)


# 时区转换
@overload
def to_tz(
    time: datetime | datetime64 | pd.Timestamp | str, tz: tzinfo | str | None
) -> pd.Timestamp: ...


@overload
def to_tz(time: pd.DataFrame, tz: tzinfo | str | None) -> pd.DataFrame: ...
@overload
def to_tz(time: pd.Series, tz: tzinfo | str | None) -> pd.Series: ...
@overload
def to_tz(time: pd.Index, tz: tzinfo | str | None) -> pd.DatetimeIndex: ...


def to_tz(
    time, tz: tzinfo | str | None = None
) -> pd.Timestamp | pd.Series | pd.Index | pd.DataFrame | pd.DatetimeIndex:

    def _set_timetz(
        time: pd.Timestamp | datetime | datetime64 | str | pd.DatetimeIndex | pd.Index,
        tz: tzinfo | str | None,
    ):
        time = pd.to_datetime(time, utc=True)
        if time.tz is None:
            if tz is None:
                return time
            return time.tz_localize(tz)
        else:
            return time.tz_convert(tz)

    match time:
        case (
            pd.Timestamp()
            | pd.DatetimeIndex()
            | datetime()
            | datetime64()
            | str()
            | pd.Index()
        ):
            return _set_timetz(time=time, tz=tz)
        case pd.Series():
            timeser = pd.to_datetime(time, utc=True)
            return timeser.dt.tz_convert(tz=tz)

        case pd.DataFrame():
            time.index = _set_timetz(time=time.index, tz=tz)
            return time
        case _:
            raise TypeError(f"Unsupported type for time: {type(time)}")


@overload
def to_utctz(time: datetime | datetime64 | pd.Timestamp | str) -> pd.Timestamp: ...


@overload
def to_utctz(time: pd.DataFrame) -> pd.DataFrame: ...
@overload
def to_utctz(time: pd.Series) -> pd.Series: ...
@overload
def to_utctz(time: pd.Index) -> pd.Index: ...


def to_utctz(time) -> pd.Timestamp | pd.DataFrame | pd.Series | pd.Index:
    return to_tz(time=time, tz=timezone.utc)


@overload
def to_nonetz(time: datetime | datetime64 | pd.Timestamp | str) -> pd.Timestamp: ...


@overload
def to_nonetz(time: pd.DataFrame) -> pd.DataFrame: ...
@overload
def to_nonetz(time: pd.Series) -> pd.Series: ...
@overload
def to_nonetz(time: pd.Index) -> pd.Index: ...
def to_nonetz(time) -> pd.Index | pd.Series | pd.DataFrame | pd.Timestamp:
    return to_tz(time=time, tz=None)


def to_stdtz(time: datetime | datetime64 | pd.Timestamp | str) -> pd.Timestamp:
    time = pd.to_datetime(time)
    return time if time.tz is None else to_utctz(time)


# 转化datetime64
@overload
def to_datetime64(para: datetime | datetime64 | pd.Timestamp | str) -> datetime64: ...


@overload
def to_datetime64(para: pd.Series) -> pd.Series: ...


@overload
def to_datetime64(para: pd.Index) -> pd.Index: ...


@overload
def to_datetime64(para: pd.DataFrame) -> pd.DataFrame: ...


def to_datetime64(para) -> pd.Series | pd.DataFrame | pd.Index | datetime64:
    if isinstance(para, datetime64):
        return para
    elif isinstance(para, datetime | pd.Timestamp | str):
        return datetime64(to_nonetz(para))
    elif isinstance(para, pd.Series | pd.Index):
        return to_tz(para, None).astype("datetime64[ns]")

    elif isinstance(para, pd.DataFrame):
        para.index = to_tz(para.index, None).astype("datetime64[ns]")
        return para
    else:
        raise TypeError(f"Unsupported type for time: {type(para)}")


@overload
def complete_timeindex(
    indexordataframe: pd.Index | pd.DatetimeIndex,
    freq: TimedeltaType | str,
    normalize=False,
) -> pd.DatetimeIndex: ...
@overload
def complete_timeindex(
    indexordataframe: pd.DataFrame, freq: TimedeltaType | str, normalize=False
) -> pd.DataFrame: ...


def complete_timeindex(
    indexordataframe: pd.DataFrame | pd.Index | pd.DatetimeIndex | pd.Series,
    freq: TimedeltaType | str,
    normalize=False,
) -> pd.DataFrame | pd.DatetimeIndex:
    if isinstance(freq, timedelta64):
        freq = pd.Timedelta(freq)
    """
    根据给定的频率(freq)补全时间索引。

    参数:
    - index: pandas的DataFrame、Index、DatetimeIndex或Series，表示需要补全的时间索引。
    - freq: 字符串，指定补全时间索引的频率，

    Alias
    D:calendar day frequency
    W:weekly frequency
    M:monthly frequency
    Q:quarterly frequency
    Y:yearly frequency
    h:hourly frequency
    min:minutely frequency
    s:secondly frequency
    ms:milliseconds
    us:microseconds
    ns:nanoseconds


    Combining aliases
    As we have seen previously, the alias and the offset instance are fungible in most functions:

    pd.date_range(start, periods=5, freq="B")
    Out[239]:
    DatetimeIndex(['2011-01-03', '2011-01-04', '2011-01-05', '2011-01-06',
                '2011-01-07'],
                dtype='datetime64[ns]', freq='B')

    pd.date_range(start, periods=5, freq=pd.offsets.BDay())
    Out[240]:
    DatetimeIndex(['2011-01-03', '2011-01-04', '2011-01-05', '2011-01-06',
                '2011-01-07'],
                dtype='datetime64[ns]', freq='B')
    You can combine together day and intraday offsets:

    pd.date_range(start, periods=10, freq="2h20min")
    Out[241]:
    DatetimeIndex(['2011-01-01 00:00:00', '2011-01-01 02:20:00',
                '2011-01-01 04:40:00', '2011-01-01 07:00:00',
                '2011-01-01 09:20:00', '2011-01-01 11:40:00',
                '2011-01-01 14:00:00', '2011-01-01 16:20:00',
                '2011-01-01 18:40:00', '2011-01-01 21:00:00'],
                dtype='datetime64[ns]', freq='140min')

    pd.date_range(start, periods=10, freq="1D10us")
    Out[242]:
    DatetimeIndex([       '2011-01-01 00:00:00', '2011-01-02 00:00:00.000010',
                '2011-01-03 00:00:00.000020', '2011-01-04 00:00:00.000030',
                '2011-01-05 00:00:00.000040', '2011-01-06 00:00:00.000050',
                '2011-01-07 00:00:00.000060', '2011-01-08 00:00:00.000070',
                '2011-01-09 00:00:00.000080', '2011-01-10 00:00:00.000090'],
                dtype='datetime64[ns]', freq='86400000010us')
    例如 'D' 表示每天，'M' 表示每月等。
    In [235]: dates_lst_1 = pd.date_range("2020-01-06", "2020-04-03", freq="MS")

    In [236]: dates_lst_1
    Out[236]: DatetimeIndex(['2020-02-01', '2020-03-01', '2020-04-01'], dtype='datetime64[ns]', freq='MS')

    In [237]: dates_lst_2 = pd.date_range("2020-01-01", "2020-04-01", freq="MS")

    In [238]: dates_lst_2
    Out[238]: DatetimeIndex(['2020-01-01', '2020-02-01', '2020-03-01', '2020-04-01'], dtype='datetime64[ns]', freq='MS')

    - normalize: 布尔值，默认为False，若为True，则会将时间索引标准化到一天的开始。

    返回值:
    - 补全后的 ‘新’ DataFrame或DatetimeIndex。

    异常:
    - 如果index的类型不是预期的类型，将抛出TypeError。
    """

    def _complete_datetimeindex(
        index: pd.DatetimeIndex | pd.Index,
        freq: pd.Timedelta | timedelta | str,
        normalize=False,
    ) -> pd.DatetimeIndex:
        if isinstance(index, pd.Index):
            index = pd.to_datetime(index)
        mintime = index.min()
        maxtime = index.max()
        if index.tz is not None:
            return pd.date_range(
                start=mintime, end=maxtime, freq=freq, normalize=normalize, tz=index.tz
            )
        else:
            return pd.date_range(
                start=mintime, end=maxtime, freq=freq, normalize=normalize
            )

    if isinstance(indexordataframe, pd.DataFrame):
        name = indexordataframe.index.name
        new_index = _complete_datetimeindex(indexordataframe.index, freq, normalize)
        new_index.name = name
        return indexordataframe.reindex(index=new_index)
    elif isinstance(indexordataframe, pd.DatetimeIndex | pd.Index):
        return _complete_datetimeindex(indexordataframe, freq, normalize)
    else:
        raise TypeError()


def parse_iso8601str(timestamp: str) -> int:
    if timestamp is None:
        return -1
    yyyy = "([0-9]{4})-?"
    mm = "([0-9]{2})-?"
    dd = "([0-9]{2})(?:T|[\\s])?"
    h = "([0-9]{2}):?"
    m = "([0-9]{2}):?"
    s = "([0-9]{2})"
    ms = "(\\.[0-9]{1,3})?"
    tz = "(?:(\\+|\\-)([0-9]{2})\\:?([0-9]{2})|Z)?"
    regex = r"" + yyyy + mm + dd + h + m + s + ms + tz
    try:
        match = re.search(regex, timestamp, re.IGNORECASE)
        if match is None:
            return -1
        yyyy, mm, dd, h, m, s, ms, sign, hours, minutes = match.groups()
        ms = ms or ".000"
        ms = (ms + "00")[0:4]
        msint = int(ms[1:])
        sign = sign or ""
        sign = int(sign + "1") * -1
        hours = int(hours or 0) * sign
        minutes = int(minutes or 0) * sign
        offset = timedelta(hours=hours, minutes=minutes)
        string = yyyy + mm + dd + h + m + s + ms + "Z"
        dt = datetime.strptime(string, "%Y%m%d%H%M%S.%fZ")
        dt = dt + offset
        return calendar.timegm(dt.utctimetuple()) * 1000 + msint
    except (TypeError, OverflowError, OSError, ValueError):
        return -1


def to_iso8601str(timestamp: int | float) -> str:
    """将时间戳转换为可读的字符串日期格式"""

    # 如果timestamp是以毫秒为单位，则先转换为秒
    if isinstance(timestamp, int):
        timestamp_seconds = timestamp / 1000.0
    else:
        # 假设float类型的timestamp已经是秒
        timestamp_seconds = timestamp
    # 使用datetime的utcfromtimestamp方法转换为UTC时间，并使用isoformat转换为ISO格式的字符串
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat()


def replace_m2min(text: str):
    pattern = r"(^|\d|\b)m(?![a-zA-Z])"
    # mg = re.match(pattern,text)
    replacement = lambda m: m.group().replace("m", "min")
    return re.sub(pattern, replacement, text)


def df2datetime_withtz(df: pd.DataFrame, key: str, tz: str | tzinfo | None = None):

    if key in df.columns:
        df.loc[:, key] = to_tz(df.loc[:, key], tz=tz)

    elif df.index.name == key:
        df.index = to_tz(df.index, tz=tz)
    else:
        raise ValueError()


def df2datetime(df: pd.DataFrame, key: str):
    if key in df.columns:
        df.loc[:, key] = pd.to_datetime(df.loc[:, key])
    elif df.index.name == key:
        df.index = pd.to_datetime(df.index)
    else:
        raise ValueError()


def datetime2str(dt: datetime | datetime64 | pd.Timestamp, format: str) -> str:
    """

    将日期时间对象或字符串转换为格式为'YYYY-MM'的字符串。

    参数:
        date_str (datetimeUnion): 可以是datetime、datetime64、pd.Timestamp对象或符合pandas to_datetime能够解析的字符串。

    返回:
        str: 格式化后的日期字符串。
    """

    if isinstance(dt, datetime64):
        dt = pd.to_datetime(dt)

    return dt.strftime(format)
