import pandas as pd

from .type import DatetimeType, TimedeltaType


class TimeFormat:
    """
    %a,当地工作日的缩写。
    Sun, Mon, ..., Sat (en_US);
    So, Mo, ..., Sa (de_DE)
    """

    week = "%a"
    """
    %A 本地化的星期中每日的完整名称。
    Sunday, Monday, ..., Saturday (en_US);
    Sonntag, Montag, ..., Samstag (de_DE)
    """
    Week = "%A"
    """
    %w 以十进制数显示的工作日，其中0表示星期日，6表示星期六。
    0, 1, ..., 6
    """
    week_6 = "%w"
    """
    %u 以十进制数显示的 ISO 8601 星期中的日序号，其中 1 表示星期一。
    1, 2, ..., 7
    """
    week_7 = "%u"
    """
    %d 补零后，以十进制数显示的月份中的一天。
    01, 02, ..., 31
    """
    Day = "%d"
    """
    %d 以十进制数显示的月份中的一天。
    1, 2, ..., 31
    """
    day = "%-d"
    """
    %b 当地月份的缩写。
    Jan, Feb, ..., Dec (en_US);
    Jan, Feb, ..., Dez (de_DE)
    """
    month = "%b"

    """
    %B 本地化的月份全名。
    January, February, ..., December (en_US);
    Januar, Februar, ..., Dezember (de_DE)
    """
    Month = "%B"

    """

    %m 补零后，以十进制数显示的月份。
    01, 02, ..., 12
    """
    Month_31 = "%m"

    """

    %-m 以十进制数显示的月份。
    1, 2, ..., 12
    """
    month_31 = "%-m"

    """
    %y 补零后，以十进制数表示的，不带世纪的年份。
    00, 01, ..., 99

    """
    year = "%y"

    """
    %Y 十进制数表示的带世纪的年份。
    0001, 0002, ..., 2013, 2014, ..., 9998, 9999

    """
    Year = "%Y"

    """
    %H 以补零后的十进制数表示的小时（24 小时制）。
    00, 01, ..., 23
    """
    Hour24 = "%H"

    """
    %-H 以补零后的十进制数表示的小时（24 小时制）。
    0, 1, ..., 23
    """
    hour24 = "%-H"

    """
    %I
    以补零后的十进制数表示的小时（12 小时制）。
    01, 02, ..., 12

    """
    Hour12 = "%I"
    """
    %-I
    十进制数表示的小时(12 小时制）。
    1, 2, ..., 12

    """
    hour12 = "%-I"
    """

    %p 本地化的 AM 或 PM 。
    AM, PM (en_US);
    am, pm (de_DE)
    """
    """

    %M 补零后，以十进制数显示的分钟。
    00, 01, ..., 59
    """
    Min = "%M"
    """
    %-M 以十进制数显示的分钟。
    0, 1, ..., 59
    """
    min = "%-M"
    """
    %S 补零后，以十进制数显示的秒。
    00, 01, ..., 59

    """
    Sec = "%S"
    """
    %-S 以十进制数显示的秒。
    0, 1, ..., 59

    """
    sec = "%-S"
    """
    %f 微秒作为一个十进制数，零填充到 6 位。
    000000, 000001, ..., 999999
    """
    MicroSec = "%f"

    """
    %z UTC 偏移量，格式为 ±HHMM[SS[.ffffff]] （如果是简单型对象则为空字符串）。
    (空),+0000, -0400, +1030, +063415, -030712.345216
    """
    zone = "%z"

    """
    %Z 时区名称（如果对象为简单型则为空字符串）。
    (空), UTC, GMT
    """
    Zone = "%Z"
    """

    %j 以补零后的十进制数表示的一年中的日序号。
    001, 002, ..., 366

    """
    """
    %U 以补零后的十进制数表示的一年中的周序号（星期日作为每周的第一天）。 在新的一年中第一个星期日之前的所有日子都被视为是在第 0 周。
    00, 01, ..., 53
    """
    """

    %W 以补零后的十进制数表示的一年中的周序号（星期一作为每周的第一天）。 在新的一年中第一个星期一之前的所有日子都被视为是在第 0 周。
    00, 01, ..., 53
    """
    """

    %c 本地化的适当日期和时间表示。
    Tue Aug 16 21:30:00 1988 (en_US);
    Di 16 Aug 21:30:00 1988 (de_DE)
    """
    """

    %x 本地化的适当日期表示。
    08/16/88 (None);
    08/16/1988 (en_US);
    16.08.1988 (de_DE)
    """
    """

    %X 本地化的适当时间表示。
    21:30:00 (en_US);
    21:30:00 (de_DE)
    (1)
    """
    """
    %% 字面的 '%' 字符。
    %
    """

    def __init__(self, datetime: DatetimeType):
        self.time: pd.Timestamp = pd.to_datetime(datetime)

    def yymmdd(self, join="-"):
        return self.time.strftime(TimeFormat.YYMMDD(join))

    def hhmmss(self, join=":"):
        return self.time.strftime(TimeFormat.HHMMSS(join))

    def dt(self, datej="-", join=" ", timej=":"):
        return self.time.strftime(TimeFormat.DT(datej, join, timej))

    @classmethod
    def YYMMDD(cls, join="-"):
        return f"{cls.Year}{join}{cls.Month_31}{join}{cls.Day}"

    @classmethod
    def HHMMSS(cls, join=":"):
        return f"{cls.Hour24}{join}{cls.Min}{join}{cls.Sec}"

    # @classmethod
    # def YYMD(cls, join="-"):
    #     return f"{cls.Year}{join}{cls.month_31}{join}{cls.day}"

    # @classmethod
    # def HMS(cls, join=":"):
    #     return f"{cls.hour24}{join}{cls.min}{join}{cls.sec}"

    @classmethod
    def DT(cls, datej="-", join=" ", timej=":"):
        return f"{cls.YYMMDD(datej)}{join}{cls.HHMMSS(timej)}"

    # @classmethod
    # def dt(cls, datej="-", join=" ", timej=":"):
    #     return f"{cls.YYMD(datej)}{join}{cls.HMS(timej)}"
