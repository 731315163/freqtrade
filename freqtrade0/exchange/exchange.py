# pragma pylint: disable=W0603
"""
Cryptocurrency Exchanges support
"""

import asyncio
import inspect
import logging
import signal
from collections.abc import Coroutine, Generator
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from math import floor, isnan
from threading import Lock
from typing import Any, Literal, TypeGuard, TypeVar

import ccxt
import ccxt.pro as ccxt_pro
from cachetools import TTLCache
from ccxt import TICK_SIZE
from dateutil import parser
from pandas import DataFrame, concat

from freqtrade.constants import (
    DEFAULT_AMOUNT_RESERVE_PERCENT,
    DEFAULT_TRADES_COLUMNS,
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
from freqtrade.data.converter import (
    clean_ohlcv_dataframe,
    ohlcv_to_dataframe,
    trades_df_remove_duplicates,
    trades_dict_to_list,
    trades_list_to_df,
)
from freqtrade.enums import (
    OPTIMIZE_MODES,
    TRADE_MODES,
    CandleType,
    MarginMode,
    PriceType,
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
    chunks,
    deep_merge_dicts,
    file_dump_json,
    file_load_json,
    safe_value_fallback2,
)
from freqtrade.util import dt_from_ts, dt_now
from freqtrade.util.datetime_helpers import dt_humanize_delta, dt_ts, format_ms_time
from freqtrade.util.periodic_cache import PeriodicCache
import freqtrade.exchange 

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Exchange(freqtrade.exchange.Exchange):
   

 
   
  

   

   

 

    @property
    def _ccxt_config(self) -> dict:
        # Parameters to add directly to ccxt sync/async initialization.
        if self.trading_mode == TradingMode.MARGIN:
            return {"options": {"defaultType": "margin"}}
        elif self.trading_mode == TradingMode.FUTURES:
            return {"options": {"defaultType": self._ft_has["ccxt_futures_name"]}}
        else:
            return {}

    @property
    def name(self) -> str:
        """exchange Name (from ccxt)"""
        return self._api.name

    @property
    def id(self) -> str:
        """exchange ccxt id"""
        return self._api.id

    @property
    def timeframes(self) -> list[str]:
        return list((self._api.timeframes or {}).keys())

    @property
    def markets(self) -> dict[str, Any]:
        """exchange ccxt markets"""
        if not self._markets:
            logger.info("Markets were not loaded. Loading them now..")
            self.reload_markets(True)
        return self._markets

    @property
    def precisionMode(self) -> int:
        """Exchange ccxt precisionMode"""
        return self._api.precisionMode

    @property
    def precision_mode_price(self) -> int:
        """
        Exchange ccxt precisionMode used for price
        Workaround for ccxt limitation to not have precisionMode for price
        if it differs for an exchange
        Might need to be updated if https://github.com/ccxt/ccxt/issues/20408 is fixed.
        """
        return self._api.precisionMode

   

   
    def _build_ohlcv_dl_jobs(
        self, pair_list: ListPairsWithTimeframes, since_ms: int | None, cache: bool
    ) -> tuple[list[Coroutine], list[PairWithTimeframe]]:
        """
        Build Coroutines to execute as part of refresh_latest_ohlcv
        """
        input_coroutines: list[Coroutine[Any, Any, OHLCVResponse]] = []
        cached_pairs = []
        for pair, timeframe, candle_type in set(pair_list):
            if timeframe not in self.timeframes and candle_type in (
                CandleType.SPOT,
                CandleType.FUTURES,
            ):
                logger.warning(
                    f"Cannot download ({pair}, {timeframe}) combination as this timeframe is "
                    f"not available on {self.name}. Available timeframes are "
                    f"{', '.join(self.timeframes)}."
                )
                continue

            if (
                (pair, timeframe, candle_type) not in self._klines
                or not cache
            ):
                input_coroutines.append(
                    self._build_coroutine(pair, timeframe, candle_type, since_ms, cache)
                )

            else:
                logger.debug(
                    f"Using cached candle (OHLCV) data for {pair}, {timeframe}, {candle_type} ..."
                )
                cached_pairs.append((pair, timeframe, candle_type))

        return input_coroutines, cached_pairs


    # fetch Trade data stuff

    def needed_candle_for_trades_ms(self, timeframe: str, candle_type: CandleType) -> int:
        candle_limit = self.ohlcv_candle_limit(timeframe, candle_type)
        tf_s = timeframe_to_seconds(timeframe)
        candles_fetched = candle_limit * self.required_candle_call_count

        max_candles = self._config["orderflow"]["max_candles"]

        required_candles = min(max_candles, candles_fetched)
        move_to = (
            tf_s * candle_limit * required_candles
            if required_candles > candle_limit
            else (max_candles + 1) * tf_s
        )

        now = timeframe_to_next_date(timeframe)
        return int((now - timedelta(seconds=move_to)).timestamp() * 1000)

    

    async def _build_trades_dl_jobs(
        self, pairwt: PairWithTimeframe, data_handler, cache: bool
    ) -> tuple[PairWithTimeframe, DataFrame | None]:
        """
        Build coroutines to refresh trades for (they're then called through async.gather)
        """
        pair, timeframe, candle_type = pairwt
        since_ms = None
        new_ticks: list = []
        all_stored_ticks_df = DataFrame(columns=[*DEFAULT_TRADES_COLUMNS, "date"])
        first_candle_ms = self.needed_candle_for_trades_ms(timeframe, candle_type)
        # refresh, if
        # a. not in _trades
        # b. no cache used
        # c. need new data
        is_in_cache = (pair, timeframe, candle_type) in self._trades
        if (
            True
        ):
            logger.debug(f"Refreshing TRADES data for {pair}")
            # fetch trades since latest _trades and
            # store together with existing trades
            try:
                until = None
                from_id = None
                if is_in_cache:
                    from_id = self._trades[(pair, timeframe, candle_type)].iloc[-1]["id"]
                    until = dt_ts()  # now

                else:
                    until = int(timeframe_to_prev_date(timeframe).timestamp()) * 1000
                    all_stored_ticks_df = data_handler.trades_load(
                        f"{pair}-cached", self.trading_mode
                    )

                    if not all_stored_ticks_df.empty:
                        if (
                            all_stored_ticks_df.iloc[-1]["timestamp"] > first_candle_ms
                            and all_stored_ticks_df.iloc[0]["timestamp"] <= first_candle_ms
                        ):
                            # Use cache and populate further
                            last_cached_ms = all_stored_ticks_df.iloc[-1]["timestamp"]
                            from_id = all_stored_ticks_df.iloc[-1]["id"]
                            # only use cached if it's closer than first_candle_ms
                            since_ms = (
                                last_cached_ms
                                if last_cached_ms > first_candle_ms
                                else first_candle_ms
                            )
                        else:
                            # Skip cache, it's too old
                            all_stored_ticks_df = DataFrame(
                                columns=[*DEFAULT_TRADES_COLUMNS, "date"]
                            )

                # from_id overrules with exchange set to id paginate
                [_, new_ticks] = await self._async_get_trade_history(
                    pair,
                    since=since_ms if since_ms else first_candle_ms,
                    until=until,
                    from_id=from_id,
                )

            except Exception:
                logger.exception(f"Refreshing TRADES data for {pair} failed")
                return pairwt, None

            if new_ticks:
                all_stored_ticks_list = all_stored_ticks_df[DEFAULT_TRADES_COLUMNS].values.tolist()
                all_stored_ticks_list.extend(new_ticks)
                trades_df = self._process_trades_df(
                    pair,
                    timeframe,
                    candle_type,
                    all_stored_ticks_list,
                    cache,
                    first_required_candle_date=first_candle_ms,
                )
                data_handler.trades_store(
                    f"{pair}-cached", trades_df[DEFAULT_TRADES_COLUMNS], self.trading_mode
                )
                return pairwt, trades_df
            else:
                logger.error(f"No new ticks for {pair}")
        return pairwt, None
