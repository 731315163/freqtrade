# pragma pylint: disable=W0603
"""
Cryptocurrency Exchanges support
"""

import asyncio
import inspect
import logging
import signal
from collections.abc import Coroutine, Generator, Iterable
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

from freqtrade0.exchange.exchange_ws import ExchangeWS
from freqtrade.constants import (  # List of pairs with their timeframes; Type for trades list; ticks, pair, timeframe, CandleType
    DEFAULT_AMOUNT_RESERVE_PERCENT,
    DEFAULT_TRADES_COLUMNS,
    NON_OPEN_EXCHANGE_STATES,
    BidAsk,
    BuySell,
    Config,
    EntryExit,
    ExchangeConfig,
    ListPairsWithTimeframes,
    ListTicksWithTimeframes,
    MakerTaker,
    OBLiteral,
    PairWithTimeframe,
    TickWithTimeframe,
    TradeList,
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
from freqtrade.exchange import exchange
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


logger = exchange.logger
T = TypeVar("T")


class Exchange(exchange.Exchange):
   
    def __init__(
        self,
        config: Config,
        *,
        exchange_config: ExchangeConfig | None = None,
        validate: bool = True,
        load_leverage_tiers: bool = False,
    ) -> None:
        """
        Initializes this module with the given config,
        it does basic validation whether the specified exchange and pairs are valid.
        :return: None
        """
        self._api: ccxt.Exchange
        self._api_async: ccxt_pro.Exchange
        self._ws_async: ccxt_pro.Exchange|None = None
        self._exchange_ws: ExchangeWS | None = None
        self._markets: dict = {}
        self._trading_fees: dict[str, Any] = {}
        self._leverage_tiers: dict[str, list[dict]] = {}
        # Lock event loop. This is necessary to avoid race-conditions when using force* commands
        # Due to funding fee fetching.
        self._loop_lock = Lock()
        self.loop = self._init_async_loop()
        self._config: Config = {}

        self._config.update(config)

        # Holds last candle refreshed time of each pair
        self._pairs_last_refresh_time: dict[PairWithTimeframe, int] = {}
        # Timestamp of last markets refresh
        self._last_markets_refresh: int = 0

        self._cache_lock = Lock()
        # Cache for 10 minutes ...
        self._fetch_tickers_cache: TTLCache = TTLCache(maxsize=4, ttl=60 * 10)
        # Cache values for 300 to avoid frequent polling of the exchange for prices
        # Caching only applies to RPC methods, so prices for open trades are still
        # refreshed once every iteration.
        # Shouldn't be too high either, as it'll freeze UI updates in case of open orders.
        self._exit_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)
        self._entry_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)

        # Holds candles
        self._klines: dict[PairWithTimeframe, DataFrame] = {}
        self._expiring_candle_cache: dict[tuple[str, int], PeriodicCache] = {}

        # Holds public_trades
        self._trades: dict[PairWithTimeframe, DataFrame] = {}

        # Holds all open sell orders for dry_run
        self._dry_run_open_orders: dict[str, Any] = {}

        if config["dry_run"]:
            logger.info("Instance is running with dry_run enabled")
        logger.info(f"Using CCXT {ccxt.__version__}")
        exchange_conf: dict[str, Any] = exchange_config if exchange_config else config["exchange"]
        remove_exchange_credentials(exchange_conf, config.get("dry_run", False))
        self.log_responses = exchange_conf.get("log_responses", False)

        # Leverage properties
        self.trading_mode: TradingMode = config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = (
            MarginMode(config.get("margin_mode")) if config.get("margin_mode") else MarginMode.NONE
        )
        self.liquidation_buffer = config.get("liquidation_buffer", 0.05)

        # Deep merge ft_has with default ft_has options
        self._ft_has = deep_merge_dicts(self._ft_has, deepcopy(self._ft_has_default))
        if self.trading_mode == TradingMode.FUTURES:
            self._ft_has = deep_merge_dicts(self._ft_has_futures, self._ft_has)
        if exchange_conf.get("_ft_has_params"):
            self._ft_has = deep_merge_dicts(exchange_conf.get("_ft_has_params"), self._ft_has)
            logger.info("Overriding exchange._ft_has with config params, result: %s", self._ft_has)

        # Assign this directly for easy access
        self._ohlcv_partial_candle = self._ft_has["ohlcv_partial_candle"]

        self._max_trades_limit = self._ft_has["trades_limit"]

        self._trades_pagination = self._ft_has["trades_pagination"]
        self._trades_pagination_arg = self._ft_has["trades_pagination_arg"]

        # Initialize ccxt objects
        ccxt_config = self._ccxt_config
        ccxt_config = deep_merge_dicts(exchange_conf.get("ccxt_config", {}), ccxt_config)
        ccxt_config = deep_merge_dicts(exchange_conf.get("ccxt_sync_config", {}), ccxt_config)

        self._api = self._init_ccxt(exchange_conf, True, ccxt_config)

        ccxt_async_config = self._ccxt_config
        ccxt_async_config = deep_merge_dicts(
            exchange_conf.get("ccxt_config", {}), ccxt_async_config
        )
        ccxt_async_config = deep_merge_dicts(
            exchange_conf.get("ccxt_async_config", {}), ccxt_async_config
        )
        self._api_async = self._init_ccxt(exchange_conf, False, ccxt_async_config)
        _has_watch_ohlcv = self.exchange_has("watchOHLCV") and self._ft_has["ws_enabled"]
        if (
            self._config["runmode"] in TRADE_MODES
            and exchange_conf.get("enable_ws", True)
            and _has_watch_ohlcv
        ):
            self._ws_async = self._init_ccxt(exchange_conf, False, ccxt_async_config)
            self._exchange_ws = ExchangeWS(self._config, self._ws_async)

        logger.info(f'Using Exchange "{self.name}"')
        self.required_candle_call_count = 1
        # Converts the interval provided in minutes in config to seconds
        self.markets_refresh_interval: int = (
            exchange_conf.get("markets_refresh_interval", 60) * 60 * 1000
        )

        if validate:
            # Initial markets load
            self.reload_markets(True, load_leverage_tiers=False)
            self.validate_config(config)
            self._startup_candle_count: int = config.get("startup_candle_count", 0)
            self.required_candle_call_count = self.validate_required_startup_candles(
                self._startup_candle_count, config.get("timeframe", "")
            )

        if self.trading_mode != TradingMode.SPOT and load_leverage_tiers:
            self.fill_leverage_tiers()
        self.additional_exchange_init()
   


    def _now_is_time_to_refresh_trades(
            self, pair: str, timeframe: str, candle_type: CandleType
        ) -> bool:  # Timeframe in seconds
            return True
    
    async def build_ohlcv_job(
        self,
        pairs_wt:ListPairsWithTimeframes,
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
        pair, timeframe, candle_type =pairs_wt
        if timeframe not in self.timeframes and candle_type in (
            CandleType.SPOT,
            CandleType.FUTURES,
        ):
            logger.warning(
                f"Cannot download ({pair}, {timeframe}) combination as this timeframe is "
                f"not available on {self.name}. Available timeframes are "
                f"{', '.join(self.timeframes)}."
            )
            return

        if (
            (pair, timeframe, candle_type) not in self._klines
            or not cache
            # or self._now_is_time_to_refresh(pair, timeframe, candle_type)
        ):
            
            res= await self._build_coroutine(pair, timeframe, candle_type, since_ms, cache)
            if isinstance(res, Exception):
                logger.warning(f"Async code raised an exception: {repr(res)}")
                return
            # Deconstruct tuple (has 5 elements)
            pair, timeframe, c_type, ticks, drop_hint = res
            drop_incomplete_ = drop_hint if drop_incomplete is None else drop_incomplete
            ohlcv_df = self._process_ohlcv_df(
                pair, timeframe, c_type, ticks, cache, drop_incomplete_
            )
            return ohlcv_df
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

    
    # Fetch historic trades

    @retrier_async
    async def _async_fetch_trades(
        self, pairs: ListPairsWithTimeframes, since: int | None = None, params: dict | None = None
    ) -> list[tuple[TradeList, Any]]:
        """
        Asynchronously gets trade history using fetch_trades.
        Handles exchange errors, does one call to the exchange.
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        returns: List of dicts containing trades, the next iteration value (new "since" or trade_id)
        """
        try:
            trades_limit = self._max_trades_limit
            # fetch trades asynchronously
            if params:
                logger.debug("Fetching trades for pair %s, params: %s ", pairs, params)
                trades = await self._api_async.fetch_trades(pairs, params=params, limit=trades_limit)
            else:
                logger.debug(
                    "Fetching trades for pair %s, since %s %s...",
                    pairs,
                    since,
                    "(" + dt_from_ts(since).isoformat() + ") " if since is not None else "",
                )
                trades = await self._api_async.fetch_trades(pairs, since=since, limit=trades_limit)
            trades = self._trades_contracts_to_amount(trades)
            pagination_value = self._get_trade_pagination_next_value(trades)
            return trades_dict_to_list(trades), pagination_value
        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching historical trade data."
                f"Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load trade history due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(f"Could not fetch trade data. Msg: {e}") from e



    def _get_trade_pagination_next_value(self, trades: list[dict]):
        """
        Extract pagination id for the next "from_id" value
        Applies only to fetch_trade_history by id.
        """
        if not trades:
            return None
        if self._trades_pagination == "id":
            return trades[-1].get("id")
        else:
            return trades[-1].get("timestamp")

    async def _async_get_trade_history_id_startup(
        self, pair: str, since: int
    ) -> tuple[TradeList, str]:
        """
        override for initial trade_history_id call
        """
        return await self._async_fetch_trades(pair, since=since)

    async def async_get_trade_history_id(
        self, pairs: list[tuple[PairWithTimeframe,str|None]], *, until: int, since: int) -> list[tuple[str, TradeList]]:
        """
        Asynchronously gets trade history using fetch_trades
        use this when exchange uses id-based iteration (check `self._trades_pagination`)
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        :param until: Until as integer timestamp in milliseconds
        :param from_id: Download data starting with ID (if id is known). Ignores "since" if set.
        returns tuple: (pair, trades-list)
        """

        trades: TradeList= []
        # DEFAULT_TRADES_COLUMNS: 0 -> timestamp
        # DEFAULT_TRADES_COLUMNS: 1 -> id
        has_overlap = self._ft_has.get("trades_pagination_overlap", True)
        # Skip last trade by default since its the key for the next call
        x = slice(None, -1) if has_overlap else slice(None)

        if not from_id or not self._valid_trade_pagination_id(pair, from_id):
            # Fetch first elements using timebased method to get an ID to paginate on
            # Depending on the Exchange, this can introduce a drift at the start of the interval
            # of up to an hour.
            # e.g. Binance returns the "last 1000" candles within a 1h time interval
            # - so we will miss the first trades.
            t, from_id = await self._async_get_trade_history_id_startup(pair, since=since)
            trades.extend(t[x])
        while True:
            try:
                t, from_id_next = await self._async_fetch_trades(
                    pair, params={self._trades_pagination_arg: from_id}
                )
                if t:
                    trades.extend(t[x])
                    if from_id == from_id_next or t[-1][0] > until:
                        logger.debug(
                            f"Stopping because from_id did not change. Reached {t[-1][0]} > {until}"
                        )
                        # Reached the end of the defined-download period - add last trade as well.
                        if has_overlap:
                            trades.extend(t[-1:])
                        break

                    from_id = from_id_next
                else:
                    logger.debug("Stopping as no more trades were returned.")
                    break
            except asyncio.CancelledError:
                logger.debug("Async operation Interrupted, breaking trades DL loop.")
                break

        return (pair, trades)

    async def async_get_trade_history_time(
        self, pairs: ListPairsWithTimeframes, until: int, since: int
    ) -> tuple[str, list[list]]:
        """
        Asynchronously gets trade history using fetch_trades,
        when the exchange uses time-based iteration (check `self._trades_pagination`)
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        :param until: Until as integer timestamp in milliseconds
        returns tuple: (pair, trades-list)
        """

        trades ={}
        for pairwithtime in pairs:
            trades[pairwithtime] = []
        # DEFAULT_TRADES_COLUMNS: 0 -> timestamp
        # DEFAULT_TRADES_COLUMNS: 1 -> id
        while True:
            try:
                t, since_next = await self._async_fetch_trades(pairs, since=since)
                if t:
                    # No more trades to download available at the exchange,
                    # So we repeatedly get the same trade over and over again.
                    if since == since_next and len(t) == 1:
                        logger.debug("Stopping because no more trades are available.")
                        break
                    since = since_next
                    trades.extend(t)
                    # Reached the end of the defined-download period
                    if until and since_next > until:
                        logger.debug(f"Stopping because until was reached. {since_next} > {until}")
                        break
                else:
                    logger.debug("Stopping as no more trades were returned.")
                    break
            except asyncio.CancelledError:
                logger.debug("Async operation Interrupted, breaking trades DL loop.")
                break

        return (pair, trades)

    async def async_get_trade_history(
        self,
        # pair with timeframe,since,until,fromid
        pairs: list[tuple[PairWithTimeframe,str|None]],
        since:int,until:int|None=None
    ) -> list[ tuple[PairWithTimeframe, TradeList]]:
        """
        Async wrapper handling downloading trades using either time or id based methods.
        """

        logger.debug(
            f"_async_get_trade_history(), pair: {pairs}, "
            f"since: {since}, until: {until}"
        )

        if until is None:
            until = ccxt.Exchange.milliseconds()
            logger.debug(f"Exchange milliseconds: {until}")

        if self._trades_pagination == "time":
            return await self.async_get_trade_history_time(pairs=pairs, since=since, until=until)
        elif self._trades_pagination == "id":
            return await self.async_get_trade_history_id(
                pairs=pairs, since=since, until=until
            )
        else:
            raise OperationalException(
                f"Exchange {self.name} does use neither time, nor id based pagination"
            )

    async def build_trades_dl_jobs(
        self, pairs_wt: Iterable[PairWithTimeframe], data_handler, cache: bool
    ) -> tuple[PairWithTimeframe, DataFrame | None]:
        """
        Build coroutines to refresh trades for (they're then called through async.gather)
        """
        pair, timeframe, candle_type = pairs_wt
        since_ms = None
        new_ticks: list = []
        all_stored_ticks_df = DataFrame(columns=[*DEFAULT_TRADES_COLUMNS, "date"])
        if pairs_wt:
            first_candle_ms = self.needed_candle_for_trades_ms(timeframe, candle_type)
        # refresh, if
        # a. not in _trades
        # b. no cache used
        # c. need new data
        is_in_cache = (pair, timeframe, candle_type) in self._trades
        if (
            not is_in_cache
            or not cache
            or self._now_is_time_to_refresh_trades(pair, timeframe, candle_type)
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
                return pairs_wt, None

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
                return pairs_wt, trades_df
            else:
                logger.error(f"No new ticks for {pair}")
        return pairs_wt, None

  
