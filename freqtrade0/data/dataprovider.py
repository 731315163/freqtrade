"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""

import asyncio
import inspect
import logging
import signal
from collections import deque
from collections.abc import Coroutine, Generator, Iterable
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from math import floor, isnan
from pdb import run
from threading import Lock
from typing import Any, Literal, TypeGuard, TypeVar

import ccxt
import ccxt.pro as ccxt_pro
from cachetools import TTLCache
from ccxt import TICK_SIZE
from dateutil import parser
from pandas import DataFrame, Timedelta, Timestamp, concat, to_timedelta

from freqtrade0.data.async_queue import Queue
from freqtrade0.exchange import Exchange
from freqtrade.configuration import TimeRange
from freqtrade.constants import (
    DEFAULT_AMOUNT_RESERVE_PERCENT,
    DEFAULT_TRADES_COLUMNS,
    FULL_DATAFRAME_THRESHOLD,
    NON_OPEN_EXCHANGE_STATES,
    BidAsk,
    BuySell,
    Config,
    EntryExit,
    ExchangeConfig,
    ListPairsWithTimeframes,
    MakerTaker,
    OBLiteral,
    PairWithTimeframe,
)
from freqtrade.data import dataprovider
from freqtrade.data.converter import (
    clean_ohlcv_dataframe,
    ohlcv_to_dataframe,
    trades_df_remove_duplicates,
    trades_dict_to_list,
    trades_list_to_df,
)
from freqtrade.data.dataprovider import MAX_DATAFRAME_CANDLES, NO_EXCHANGE_EXCEPTION, logger
from freqtrade.data.history import get_datahandler, load_pair_history
from freqtrade.enums import (
    OPTIMIZE_MODES,
    TRADE_MODES,
    CandleType,
    MarginMode,
    PriceType,
    RPCMessageType,
    RunMode,
    TradingMode,
)
from freqtrade.exceptions import (
    ConfigurationError,
    DDosProtection,
    ExchangeError,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    PricingError,
    RetryableOrderError,
    TemporaryError,
)
from freqtrade.exchange import timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.exchange.common import (
    API_FETCH_ORDER_RETRY_COUNT,
    remove_exchange_credentials,
    retrier,
    retrier_async,
)
from freqtrade.exchange.exchange_types import (
    CcxtBalances,
    CcxtOrder,
    CcxtPosition,
    FtHas,
    OHLCVResponse,
    OrderBook,
    Ticker,
    Tickers,
)
from freqtrade.exchange.exchange_utils import (
    ROUND,
    ROUND_DOWN,
    ROUND_UP,
    amount_to_contract_precision,
    amount_to_contracts,
    amount_to_precision,
    contracts_to_amount,
    date_minus_candles,
    is_exchange_known_ccxt,
    market_is_active,
    price_to_precision,
)
from freqtrade.exchange.exchange_utils_timeframe import (
    timeframe_to_minutes,
    timeframe_to_msecs,
    timeframe_to_next_date,
    timeframe_to_prev_date,
    timeframe_to_seconds,
)
from freqtrade.exchange.exchange_ws import ExchangeWS
from freqtrade.misc import (
    append_candles_to_dataframe,
    chunks,
    deep_merge_dicts,
    file_dump_json,
    file_load_json,
    safe_value_fallback2,
)
from freqtrade.rpc import RPCManager
from freqtrade.rpc.rpc_types import RPCAnalyzedDFMsg
from freqtrade.util import PeriodicCache, dt_from_ts, dt_now
from freqtrade.util.datetime_helpers import dt_humanize_delta, dt_ts, format_ms_time
from freqtrade.util.periodic_cache import PeriodicCache


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
        pair_wt: PairWithTimeframe,
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
     
        pair, timeframe, candle_type =pair_wt
        if timeframe not in self._exchange.timeframes and candle_type in ( 
            CandleType.SPOT,
            CandleType.FUTURES,
        ):
            logger.warning(
                f"Cannot download ({pair}, {timeframe}) combination as this timeframe is "
                f"not available on {self._exchange.name}. Available timeframes are "
                f"{', '.join(self._exchange.timeframes)}."
            )
            return

        if (
            (pair, timeframe, candle_type) not in self._exchange._klines
            or not cache
            or self._now_is_time_to_refresh(pair, timeframe, candle_type)
        ):
            
            res = await self._exchange._build_coroutine(pair, timeframe, candle_type, since_ms, cache)
            if isinstance(res, Exception):
                logger.warning(f"Async code raised an exception: {repr(res)}")
                return
            # Deconstruct tuple (has 5 elements)
            pair, timeframe, c_type, ticks, drop_hint = res
            drop_incomplete_ = drop_hint if drop_incomplete is None else drop_incomplete
            ohlcv_df = self._exchange._process_ohlcv_df(
                pair, timeframe, c_type, ticks, cache, drop_incomplete_
            )
            return ohlcv_df
  
   
        
            
         
       
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
        await self._exchange.build_trades_dl_jobs(pairs_wt=pairs_wt, data_handler=data_handler, cache=cache)
    # async def gather_coroutines_latest_trades(self, pairlist: ListPairsWithTimeframes) :
    #     """
    #     Refresh latest trades data (if enabled in config)
    #     """
    #     if self._exchange is None:
    #         raise OperationalException(NO_EXCHANGE_EXCEPTION) 
   
    #     for pair_wt in self.refresh_trades_running_tasks:
    #         pairlist.remove(pair_wt)
    #     for pair_wt in pairlist :
    #         if pair_wt not in self.refresh_trades_queue:
    #             await self.refresh_trades_queue.put(self.build_coroutine_latest_trade(pair_wt,data_handler=data_handler,running_queue=self.refresh_trades_running_tasks))
       
        




    # def refresh_latest_trades(
    #     self,
    #     pair_list: ListPairsWithTimeframes,
    #     *,
    #     cache: bool = True,
    # ) -> dict[PairWithTimeframe, DataFrame]:
    #     """
    #     Refresh in-memory TRADES asynchronously and set `_trades` with the result
    #     Loops asynchronously over pair_list and downloads all pairs async (semi-parallel).
    #     Only used in the dataprovider.refresh() method.
    #     :param pair_list: List of 3 element tuples containing (pair, timeframe, candle_type)
    #     :param cache: Assign result to _trades. Useful for one-off downloads like for pairlists
    #     :return: Dict of [{(pair, timeframe): Dataframe}]
    #     """
    #     from freqtrade.data.history import get_datahandler

    #     data_handler = get_datahandler(
    #         self._config["datadir"], data_format=self._config["dataformat_trades"]
    #     )
    #     logger.debug("Refreshing TRADES data for %d pairs", len(pair_list))
    #     results_df = {}
    #     trades_dl_jobs = []
    #     for pair_wt in set(pair_list):
    #         trades_dl_jobs.append(self._build_trades_dl_jobs(pair_wt, data_handler, cache))

    #     async def gather_coroutines(coro):
    #         return await asyncio.gather(*coro, return_exceptions=True)

    #     for dl_job_chunk in chunks(trades_dl_jobs, 100):
    #         with self._loop_lock:
    #             results = self.loop.run_until_complete(gather_coroutines(dl_job_chunk))

    #         for res in results:
    #             if isinstance(res, Exception):
    #                 logger.warning(f"Async code raised an exception: {repr(res)}")
    #                 continue
    #             pairwt, trades_df = res
    #             if trades_df is not None:
    #                 results_df[pairwt] = trades_df

    #     return results_df

         