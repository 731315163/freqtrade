"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""

import asyncio
import asyncio.taskgroups
import logging
from collections import deque
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, TypeVar

import polars as pl
from pandas import DataFrame
from tradepulse.exchange import ExchangeABC, ExchangeFactory

from freqtrade.configuration import TimeRange
from freqtrade.constants import (
    Config,
    ListPairsWithTimeframes,
    PairWithTimeframe,
)
from freqtrade.data import dataprovider
from freqtrade.data.dataprovider import MAX_DATAFRAME_CANDLES, NO_EXCHANGE_EXCEPTION, logger
from freqtrade.data.history import get_datahandler, load_pair_history
from freqtrade.enums import (
    CandleType,
    RunMode,
    TradingMode,
)
from freqtrade.exceptions import (
    ExchangeError,
    OperationalException,
)
from freqtrade.exchange import timeframe_to_msecs, timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.exchange.exchange_types import (
    OrderBook,
)
from freqtrade.rpc import RPCManager
from freqtrade.util import PeriodicCache
from freqtrade.util.datetime_helpers import dt_ts
from freqtrade0.exchange import Exchange


"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""



from freqtrade.exchange import Exchange, timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.util import PeriodicCache


logger = logging.getLogger(__name__)

NO_EXCHANGE_EXCEPTION = "Exchange is not available to DataProvider."
MAX_DATAFRAME_CANDLES = 1000


logger = logging.getLogger(__name__)

T = TypeVar("T")
class DataProvider(dataprovider.DataProvider):
    def __init__(
        self,
        config: Config,
        exchange: Exchange | None,
        pairlists=None,
        rpc: RPCManager | None = None,
    ) -> None:
        self._config = config
        self._exchange = exchange
        self._pairlists = pairlists
        self.__rpc = rpc
        self.__cached_pairs: dict[PairWithTimeframe, tuple[DataFrame, datetime]] = {}
        self.__slice_index: dict[str, int] = {}
        self.__slice_date: datetime | None = None

        self.__cached_pairs_backtesting: dict[PairWithTimeframe, DataFrame] = {}
        self.__producer_pairs_df: dict[
            str, dict[PairWithTimeframe, tuple[DataFrame, datetime]]
        ] = {}
        self.__producer_pairs: dict[str, list[str]] = {}
        self._msg_queue: deque = deque()

        self._default_candle_type = self._config.get("candle_type_def", CandleType.SPOT)
        self._default_timeframe = self._config.get("timeframe", "1h")

        self.__msg_cache = PeriodicCache(
            maxsize=1000, ttl=timeframe_to_seconds(self._default_timeframe)
        )

        self.producers = self._config.get("external_message_consumer", {}).get("producers", [])
        self.external_data_enabled = len(self.producers) > 0

        # custom define
        exchange_name = exchange.name if exchange else""
        if exchange_name is None or exchange_name.strip() == "":
          exchange_name = self._config.get("exchange", {}).get("name", "binance")
        self._exchangeABC: ExchangeABC = ExchangeFactory.get_exchange(exchange_name,config=config)
    def _now_is_time_to_refresh_trades(
            self, pair: str, timeframe: str, candle_type: CandleType
        ) -> bool:  # Timeframe in seconds
            return True

    def _now_is_time_to_refresh(self, pair: str, timeframe: str, candle_type: CandleType) -> bool:
        # Timeframe in seconds
        interval_in_sec = timeframe_to_msecs(timeframe)
        plr = self._exchange. _pairs_last_refresh_time.get((pair, timeframe, candle_type), 0) + interval_in_sec
        # current,active candle open date
        now = dt_ts(timeframe_to_prev_date(timeframe))
        return plr < now
    async def build_ohlcv_job(
        self,
        pairs_wt: Iterable[PairWithTimeframe],
        *,
        since_ms: int | None = None,
        cache: bool = True,
        drop_incomplete: bool | None = None,
    ) :
        """
        Refresh in-memory OHLCV asynchronously and set `_klines` with the result
        Loops asynchronously over pair_list and downloads all pairs async (semi-parallel).
        Only used in the dataprovider.refresh() method.
        :param pair_list: List of 2 element tuples containing pair, interval to refresh
        :param since_ms: time since when to download, in milliseconds
        :param cache: Assign result to _klines. Useful for one-off downloads like for pairlists
        :param drop_incomplete: Control candle dropping.
            Specifying None defaults to _ohlcv_partial_candle
        :return: Dict of [{(pair, timeframe): Dataframe}]

        Build Coroutines to execute as part of refresh_latest_ohlcv
        """
        async with asyncio.taskgroups.TaskGroup() as tg:
            for pair_wt in pairs_wt:
                pair, timeframe, candle_type = pair_wt
                tg.create_task( self._exchangeABC.ohlcv(symbol = pair,timeframe=timeframe, since=0 ,marketType= candle_type))

    async def build_trades_job(
        self,
        pairs_wt: Iterable[PairWithTimeframe],
        cache: bool = True,
    ) :
        """
        Refresh in-memory TRADES asynchronously and set `_trades` with the result
        Loops asynchronously over pair_list and downloads all pairs async (semi-parallel).
        Only used in the dataprovider.refresh() method.
        :param pair_list: List of 3 element tuples containing (pair, timeframe, candle_type)
        :param cache: Assign result to _trades. Useful for one-off downloads like for pairlists
        :return: Dict of [{(pair, timeframe): Dataframe}]
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        from freqtrade.data.history import get_datahandler
        data_handler = get_datahandler(
        self._config["datadir"], data_format=self._config["dataformat_trades"]
        )
        async with asyncio.taskgroups.TaskGroup() as tg:
            for pair_wt in pairs_wt:
                pair, timeframe, candle_type = pair_wt
                tg.create_task( self._exchangeABC.trades(symbol = pair,since = 0,marketType = candle_type))




    def get_producer_pairs(self, producer_name: str = "default") -> list[str]:
        """
        Get the pairs cached from the producer

        :returns: List of pairs
        """
        return self.__producer_pairs.get(producer_name, []).copy()



    def get_producer_df(
        self,
        pair: str,
        timeframe: str | None = None,
        candle_type: CandleType | None = None,
        producer_name: str = "default",
    ) -> tuple[DataFrame, datetime]:
        """
        Get the pair data from producers.

        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        :returns: Tuple of the DataFrame and last analyzed timestamp
        """
        _timeframe = self._default_timeframe if not timeframe else timeframe
        _candle_type = self._default_candle_type if not candle_type else candle_type

        pair_key = (pair, _timeframe, _candle_type)

        # If we have no data from this Producer yet
        if producer_name not in self.__producer_pairs_df:
            # We don't have this data yet, return empty DataFrame and datetime (01-01-1970)
            return (DataFrame(), datetime.fromtimestamp(0, tz=UTC))

        # If we do have data from that Producer, but no data on this pair_key
        if pair_key not in self.__producer_pairs_df[producer_name]:
            # We don't have this data yet, return empty DataFrame and datetime (01-01-1970)
            return (DataFrame(), datetime.fromtimestamp(0, tz=UTC))

        # We have it, return this data
        df, la = self.__producer_pairs_df[producer_name][pair_key]
        return (df.copy(), la)

    def add_pairlisthandler(self, pairlists) -> None:
        """
        Allow adding pairlisthandler after initialization
        """
        self._pairlists = pairlists

    def historic_ohlcv(self, pair: str, timeframe: str, candle_type: str = "") -> DataFrame:
        """
        Get stored historical candle (OHLCV) data
        :param pair: pair to get the data for
        :param timeframe: timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        """
        _candle_type = (
            CandleType.from_string(candle_type)
            if candle_type != ""
            else self._config["candle_type_def"]
        )
        saved_pair: PairWithTimeframe = (pair, str(timeframe), _candle_type)
        if saved_pair not in self.__cached_pairs_backtesting:
            timerange = TimeRange.parse_timerange(
                None
                if self._config.get("timerange") is None
                else str(self._config.get("timerange"))
            )

            startup_candles = self.get_required_startup(str(timeframe))
            tf_seconds = timeframe_to_seconds(str(timeframe))
            timerange.subtract_start(tf_seconds * startup_candles)

            logger.info(
                f"Loading data for {pair} {timeframe} "
                f"from {timerange.start_fmt} to {timerange.stop_fmt}"
            )

            self.__cached_pairs_backtesting[saved_pair] = load_pair_history(
                pair=pair,
                timeframe=timeframe,
                datadir=self._config["datadir"],
                timerange=timerange,
                data_format=self._config["dataformat_ohlcv"],
                candle_type=_candle_type,
            )
        return self.__cached_pairs_backtesting[saved_pair].copy()

    def get_required_startup(self, timeframe: str) -> int:
        freqai_config = self._config.get("freqai", {})
        if not freqai_config.get("enabled", False):
            return self._config.get("startup_candle_count", 0)
        else:
            startup_candles = self._config.get("startup_candle_count", 0)
            indicator_periods = freqai_config["feature_parameters"]["indicator_periods_candles"]
            # make sure the startupcandles is at least the set maximum indicator periods
            self._config["startup_candle_count"] = max(startup_candles, max(indicator_periods))
            tf_seconds = timeframe_to_seconds(timeframe)
            train_candles = freqai_config["train_period_days"] * 86400 / tf_seconds
            total_candles = int(self._config["startup_candle_count"] + train_candles)
            logger.info(
                f"Increasing startup_candle_count for freqai on {timeframe} to {total_candles}"
            )
        return total_candles

    def get_pair_dataframe(
        self, pair: str, timeframe: str | None = None, candle_type: str = ""
    ) -> DataFrame:
        """
        Return pair candle (OHLCV) data, either live or cached historical -- depending
        on the runmode.
        Only combinations in the pairlist or which have been specified as informative pairs
        will be available.
        :param pair: pair to get the data for
        :param timeframe: timeframe to get data for
        :return: Dataframe for this pair
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        """
        if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
            # Get live OHLCV data.
            data = self.ohlcv(pair=pair, timeframe=timeframe, candle_type=candle_type)
        else:
            # Get historical OHLCV data (cached on disk).
            timeframe = timeframe or self._config["timeframe"]
            data = self.historic_ohlcv(pair=pair, timeframe=timeframe, candle_type=candle_type)
            # Cut date to timeframe-specific date.
            # This is necessary to prevent lookahead bias in callbacks through informative pairs.
            if self.__slice_date:
                cutoff_date = timeframe_to_prev_date(timeframe, self.__slice_date)
                data = data.loc[data["date"] < cutoff_date]
        if len(data) == 0:
            logger.warning(f"No data found for ({pair}, {timeframe}, {candle_type}).")
        return data

    def get_analyzed_dataframe(self, pair: str, timeframe: str) -> tuple[DataFrame, datetime]:
        """
        Retrieve the analyzed dataframe. Returns the full dataframe in trade mode (live / dry),
        and the last 1000 candles (up to the time evaluated at this moment) in all other modes.
        :param pair: pair to get the data for
        :param timeframe: timeframe to get data for
        :return: Tuple of (Analyzed Dataframe, lastrefreshed) for the requested pair / timeframe
            combination.
            Returns empty dataframe and Epoch 0 (1970-01-01) if no dataframe was cached.
        """
        pair_key = (pair, timeframe, self._config.get("candle_type_def", CandleType.SPOT))
        if pair_key in self.__cached_pairs:
            if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
                df, date = self.__cached_pairs[pair_key]
            else:
                df, date = self.__cached_pairs[pair_key]
                if (max_index := self.__slice_index.get(pair)) is not None:
                    df = df.iloc[max(0, max_index - MAX_DATAFRAME_CANDLES) : max_index]
                else:
                    return (DataFrame(), datetime.fromtimestamp(0, tz=UTC))
            return df, date
        else:
            return (DataFrame(), datetime.fromtimestamp(0, tz=UTC))

    @property
    def runmode(self) -> RunMode:
        """
        Get runmode of the bot
        can be "live", "dry-run", "backtest", "edgecli", "hyperopt" or "other".
        """
        return RunMode(self._config.get("runmode", RunMode.OTHER))

    def current_whitelist(self) -> list[str]:
        """
        fetch latest available whitelist.

        Useful when you have a large whitelist and need to call each pair as an informative pair.
        As available pairs does not show whitelist until after informative pairs have been cached.
        :return: list of pairs in whitelist
        """

        if self._pairlists:
            return self._pairlists.whitelist.copy()
        else:
            raise OperationalException("Dataprovider was not initialized with a pairlist provider.")

    def clear_cache(self):
        """
        Clear pair dataframe cache.
        """
        self.__cached_pairs = {}
        # Don't reset backtesting pairs -
        # otherwise they're reloaded each time during hyperopt due to with analyze_per_epoch
        # self.__cached_pairs_backtesting = {}
        self.__slice_index = {}





    def refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> None:
        """
        Refresh latest trades data (if enabled in config)
        """

        use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
        if use_public_trades:
            if self._exchange:
                self._exchange.refresh_latest_trades(pairlist)

    @property
    def available_pairs(self) -> ListPairsWithTimeframes:
        """
        Return a list of tuples containing (pair, timeframe) for which data is currently cached.
        Should be whitelist + open trades.
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        return list(self._exchange._klines.keys())

    def ohlcv(
        self, pair: str, timeframe: str | None = None, copy: bool = True, candle_type: str = ""
    ) -> DataFrame:
        
        """
        Get candle (OHLCV) data for the given pair as DataFrame
        Please use the `available_pairs` method to verify which pairs are currently cached.
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param copy: copy dataframe before returning if True.
                     Use False only for read-only operations (where the dataframe is not modified)
        """
        return DataFrame()
        # if self._exchange is None:
        #     raise OperationalException(NO_EXCHANGE_EXCEPTION)
        # if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
        #     _candle_type = (
        #         CandleType.from_string(candle_type)
        #         if candle_type != ""
        #         else self._config["candle_type_def"]
        #     )
            
        #     match _candle_type: 
        #       case CandleType.SPOT |  CandleType.FUTURES:
        #         dataframe:pl.DataFrame = await self._exchangeABC.ohlcv(symbol=pair,timeframe= timeframe or self._config["timeframe"],since=0,marketType=str(_candle_type))
        #       case CandleType.FUNDING_RATE:
        #         dataframe = self._exchange.funding_rates(symbol=pair,timeframe= timeframe or self._config["timeframe"],since=0)
        #       case _:
        #             raise TypeError(f"Invalid candle_type: {candle_type}")
        #     df = dataframe.to_pandas()
        #     return self._exchange.klines(
        #         (pair, timeframe or self._config["timeframe"], _candle_type), copy=copy
        #     )
        # else:
        #     return DataFrame()

    def trades(
        self, pair: str, timeframe: str | None = None, copy: bool = True, candle_type: str = ""
    ) -> DataFrame:
        """
        Get candle (TRADES) data for the given pair as DataFrame
        Please use the `available_pairs` method to verify which pairs are currently cached.
        This is not meant to be used in callbacks because of lookahead bias.
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param copy: copy dataframe before returning if True.
                     Use False only for read-only operations (where the dataframe is not modified)
        """
        if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
            if self._exchange is None:
                raise OperationalException(NO_EXCHANGE_EXCEPTION)
            _candle_type = (
                CandleType.from_string(candle_type)
                if candle_type != ""
                else self._config["candle_type_def"]
            )
            return self._exchange.trades(
                (pair, timeframe or self._config["timeframe"], _candle_type), copy=copy
            )
        else:
            data_handler = get_datahandler(
                self._config["datadir"], data_format=self._config["dataformat_trades"]
            )
            trades_df = data_handler.trades_load(
                pair, self._config.get("trading_mode", TradingMode.SPOT)
            )
            return trades_df

    def market(self, pair: str) -> dict[str, Any] | None:
        """
        Return market data for the pair
        :param pair: Pair to get the data for
        :return: Market data dict from ccxt or None if market info is not available for the pair
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        return self._exchange.markets.get(pair)

    def ticker(self, pair: str):
        """
        Return last ticker data from exchange
        :param pair: Pair to get the data for
        :return: Ticker dict from exchange or empty dict if ticker is not available for the pair
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        try:
            return self._exchange.fetch_ticker(pair)
        except ExchangeError:
            return {}

    def orderbook(self, pair: str, maximum: int) -> OrderBook:
        """
        Fetch latest l2 orderbook data
        Warning: Does a network request - so use with common sense.
        :param pair: pair to get the data for
        :param maximum: Maximum number of orderbook entries to query
        :return: dict including bids/asks with a total of `maximum` entries.
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        return self._exchange.fetch_l2_order_book(pair, maximum)

