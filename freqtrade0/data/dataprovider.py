"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""

import asyncio
from copy import deepcopy
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

from pandas import DataFrame, Timedelta, Timestamp, to_timedelta

from freqtrade.configuration import TimeRange
from freqtrade.constants import (
    FULL_DATAFRAME_THRESHOLD,
    Config,
    ListPairsWithTimeframes,
    PairWithTimeframe,
)
from freqtrade.data.history import get_datahandler, load_pair_history
from freqtrade.enums import CandleType, RPCMessageType, RunMode, TradingMode
from freqtrade.exceptions import ExchangeError, OperationalException
from freqtrade.exchange import  timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.exchange.exchange_types import OrderBook
from freqtrade.misc import append_candles_to_dataframe
from freqtrade.rpc import RPCManager
from freqtrade.rpc.rpc_types import RPCAnalyzedDFMsg
from freqtrade.util import PeriodicCache
from freqtrade.data import dataprovider
from freqtrade0.exchange import Exchange
logger = logging.getLogger(__name__)

NO_EXCHANGE_EXCEPTION = "Exchange is not available to DataProvider."
MAX_DATAFRAME_CANDLES = 1000


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



    def merge_pairs_helperpairs(self,pairlist: ListPairsWithTimeframes,
        helping_pairs: ListPairsWithTimeframes | None = None):
        final_pairs = (pairlist + helping_pairs) if helping_pairs else pairlist
        return final_pairs
    # Exchange functions

    def refresh(
        self,
        pairlist: ListPairsWithTimeframes,
        helping_pairs: ListPairsWithTimeframes | None = None,
    ) -> None:
        """
        Refresh data, called with each cycle
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        final_pairs = self.merge_pairs_helperpairs(pairlist,helping_pairs)
        # refresh latest ohlcv data
        self._exchange.refresh_latest_ohlcv(final_pairs)
        # refresh latest trades data
        self.refresh_latest_trades(pairlist)
    def refresh_latest_ohlcv(self, pairlist: ListPairsWithTimeframes)->None:
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        self._exchange.refresh_latest_ohlcv(pair_list=pairlist)

    def refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> None:
        """
        Refresh latest trades data (if enabled in config)
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
        if use_public_trades:
            self._exchange.refresh_latest_trades(pairlist)
    # async def async_refresh_latest_ohlcv(self, pairlist: ListPairsWithTimeframes) -> dict:
    #     """
    #     Refresh latest ohlcv data (if enabled in config)
    #     """
    #     if self._exchange is None:
    #         raise OperationalException(NO_EXCHANGE_EXCEPTION)
    #     ohlcv_refresh_time={}
       
    #     last_refresh_time=deepcopy(self._exchange._pairs_last_refresh_time)
    #     await  self._exchange.refresh_latest_ohlcv(pairlist)
    #     for pair_timeframe,time in self._exchange._pairs_last_refresh_time.items():
    #         if pair_timeframe in last_refresh_time and last_refresh_time[pair_timeframe] < time:
    #             ohlcv_refresh_time[pair_timeframe]=time
    #     return ohlcv_refresh_time
    # async def async_refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> dict:
    #     """
    #     Refresh latest trades data (if enabled in config)
    #     """
    #     use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
    #     if use_public_trades and self._exchange:
    #         return await self._exchange.refresh_latest_trades(pairlist)
    #     else:
    #         return {}