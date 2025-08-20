# pragma pylint: disable=missing-docstring, W0212, too-many-arguments

"""
This module contains the backtesting logic
"""

import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

from numpy import isnan, nan
from pandas import DataFrame, Series

import freqtrade
from freqtrade import constants
from freqtrade.configuration import TimeRange, validate_config_consistency
from freqtrade.constants import DATETIME_PRINT_FORMAT, Config, IntOrInf, LongShort
from freqtrade.data import history
from freqtrade.data.btanalysis import (
    find_existing_backtest_stats,
    get_tick_size_over_time,
    trade_list_to_dataframe,
)
from freqtrade.data.converter import trim_dataframe, trim_dataframes
from freqtrade.data.dataprovider import DataProvider
from freqtrade.data.metrics import combined_dataframes_with_rel_mean
from freqtrade.enums import (
    BacktestState,
    CandleType,
    ExitCheckTuple,
    ExitType,
    MarginMode,
    RunMode,
    TradingMode,
)
from freqtrade.exceptions import DependencyException, OperationalException
from freqtrade.exchange import (
    amount_to_contract_precision,
    price_to_precision,
    timeframe_to_seconds,
)
from freqtrade.exchange.exchange import TICK_SIZE, Exchange
from freqtrade.ft_types import (
    BacktestContentType,
    BacktestContentTypeIcomplete,
    BacktestResultType,
    get_BacktestResultType_default,
)
from freqtrade.leverage.liquidation_price import update_liquidation_prices
from freqtrade.mixins import LoggingMixin
from freqtrade.optimize.backtest_caching import get_strategy_run_id
from freqtrade.optimize.bt_progress import BTProgress
from freqtrade.optimize.optimize_reports import (
    generate_backtest_stats,
    generate_rejected_signals,
    generate_trade_signal_candles,
    show_backtest_results,
    store_backtest_results,
)
from freqtrade.persistence import (
    CustomDataWrapper,
    LocalTrade,
    Order,
    PairLocks,
    Trade,
    disable_database_use,
    enable_database_use,
)
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, dt_now
from freqtrade.util.migrations import migrate_data
from freqtrade.wallets import Wallets
from freqtrade0.resolvers import ExchangeResolver, StrategyResolver
from freqtrade0.strategy.interface import IStrategy


logger = logging.getLogger(__name__)

# Indexes for backtest tuples
DATE_IDX = 0
OPEN_IDX = 1
HIGH_IDX = 2
LOW_IDX = 3
CLOSE_IDX = 4
LONG_IDX = 5
ELONG_IDX = 6  # Exit long
SHORT_IDX = 7
ESHORT_IDX = 8  # Exit short
ENTER_TAG_IDX = 9
EXIT_TAG_IDX = 10

# Every change to this headers list must evaluate further usages of the resulting tuple
# and eventually change the constants for indexes at the top
HEADERS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "enter_long",
    "exit_long",
    "enter_short",
    "exit_short",
    "enter_tag",
    "exit_tag",
]


class Backtesting(freqtrade.optimize.backtesting.Backtesting):
    """
    Backtesting class, this class contains all the logic to run a backtest

    To run a backtest:
    backtesting = Backtesting(config)
    backtesting.start()
    """

    def __init__(self, config: Config, exchange: Exchange | None = None) -> None:
        super().__init__(config, exchange)
        self.dataprovider = DataProvider(self.config, self.exchange)


        self.dataprovider.add_pairlisthandler(self.pairlists)
        self.pairlists.refresh_pairlist()

        self.init_backtest()



    @staticmethod
    def cleanup():
        LoggingMixin.show_output = True
        enable_database_use()




    def _set_strategy(self, strategy: IStrategy):
        """
        Load strategy into backtesting
        """
        self.strategy: IStrategy = strategy
        strategy.dp = self.dataprovider
        # Attach Wallets to Strategy baseclass
        strategy.wallets = self.wallets
        # Set stoploss_on_exchange to false for backtesting,
        # since a "perfect" stoploss-exit is assumed anyway
        # And the regular "stoploss" function would not apply to that case
        self.strategy.order_types["stoploss_on_exchange"] = False
        # Update can_short flag
        self._can_short = self.trading_mode != TradingMode.SPOT and strategy.can_short

        self.strategy.ft_bot_start()

    def _load_protections(self, strategy: IStrategy):
        if self.config.get("enable_protections", False):
            self.protections = ProtectionManager(self.config, strategy.protections)




  

    #TODO
    def _check_adjust_trade_for_candle(
        self, trade: LocalTrade, row: tuple, current_time: datetime
    ) -> LocalTrade:
        '''
        需要与freqtrade 中的check_and_call_adjust_trade_position方法一致
        '''
        current_rate: float = row[OPEN_IDX]
        current_profit = trade.calc_profit_ratio(current_rate)
        min_stake = self.exchange.get_min_pair_stake_amount(trade.pair, current_rate, -0.1)
        max_stake = self.exchange.get_max_pair_stake_amount(trade.pair, current_rate)
        stake_available = self.wallets.get_available_stake_amount()
        stake_amount, order_tag = self.strategy._adjust_trade_position_internal(
            trade=trade,  # type: ignore[arg-type]
            current_time=current_time,
            current_rate=current_rate,
            current_profit=current_profit,
            min_stake=min_stake,
            max_stake=min(max_stake, stake_available),
            current_entry_rate=current_rate,
            current_exit_rate=current_rate,
            current_entry_profit=current_profit,
            current_exit_profit=current_profit,
        )

        # Check if we should increase our position
        if stake_amount is not None and stake_amount > 0.0:
            check_adjust_entry = True
            if self.strategy.max_entry_position_adjustment > -1:
                entry_count = trade.nr_of_successful_entries
                check_adjust_entry = entry_count <= self.strategy.max_entry_position_adjustment
            if check_adjust_entry:
                pos_trade = self._enter_trade(
                    trade.pair,
                    row,
                    "short" if trade.is_short else "long",
                    stake_amount,
                    trade,
                    entry_tag1=order_tag,
                )
                if pos_trade is not None:
                    self.wallets.update()
                    return pos_trade

        if stake_amount is not None and stake_amount < 0.0:
            amount = amount_to_contract_precision(
                abs(
                    float(
                        FtPrecise(stake_amount)
                        * FtPrecise(trade.amount)
                        / FtPrecise(trade.stake_amount)
                    )
                ),
                trade.amount_precision,
                self.precision_mode,
                trade.contract_size,
            )
            if amount == 0.0:
                return trade
            remaining = (trade.amount - amount) * current_rate
            if min_stake and remaining != 0 and remaining < min_stake:
                # Remaining stake is too low to be sold.
                return trade
            exit_ = ExitCheckTuple(ExitType.PARTIAL_EXIT, order_tag)
            pos_trade = self._get_exit_for_signal(trade, row, exit_, current_time, amount)
            if pos_trade is not None:
                order = pos_trade.orders[-1]
                # If the order was filled and for the full trade amount, we need to close the trade.
                self._process_exit_order(order, pos_trade, current_time, row, trade.pair)
                return pos_trade

        return trade







    def cancel_open_orders(self, trade: LocalTrade, current_time: datetime):
        """
        Cancel all open orders for the given trade.
        """
        for order in [o for o in trade.orders if o.ft_is_open]:
            if order.side == trade.entry_side:
                self.canceled_entry_orders += 1
            elif order.side == trade.exit_side:
                self.canceled_exit_orders += 1
            # canceled orders are removed from the trade
            del trade.orders[trade.orders.index(order)]

    def handle_similar_order(
        self, trade: LocalTrade, price: float, amount: float, side: str, current_time: datetime
    ) -> bool:
        """
        Handle similar order for the given trade.
        """
        if trade.has_open_orders:
            oo = trade.select_order(side, True)
            if oo:
                if (price == oo.price) and (side == oo.side) and (amount == oo.amount):
                    # logger.info(
                    #     f"A similar open order was found for {trade.pair}. "
                    #     f"Keeping existing {trade.exit_side} order. {price=},  {amount=}"
                    # )
                    return True
            self.cancel_open_orders(trade, current_time)

        return False

    def check_order_cancel(
        self, trade: LocalTrade, order: Order, current_time: datetime
    ) -> bool | None:
        """
        Check if current analyzed order has to be canceled.
        Returns True if the trade should be Deleted (initial order was canceled),
                False if it's Canceled
                None if the order is still active.
        """
        timedout = self.strategy.ft_check_timed_out(
            trade,  # type: ignore[arg-type]
            order,
            current_time,
        )
        if timedout:
            if order.side == trade.entry_side:
                self.timedout_entry_orders += 1
                if trade.nr_of_successful_entries == 0:
                    # Remove trade due to entry timeout expiration.
                    return True
                else:
                    # Close additional entry order
                    del trade.orders[trade.orders.index(order)]
                    return False
            if order.side == trade.exit_side:
                self.timedout_exit_orders += 1
                # Close exit order and retry exiting on next signal.
                del trade.orders[trade.orders.index(order)]
                return False
        return None

    def check_order_replace(
        self, trade: LocalTrade, order: Order, current_time, row: tuple
    ) -> bool:
        """
        Check if current analyzed entry order has to be replaced and do so.
        If user requested cancellation and there are no filled orders in the trade will
        instruct caller to delete the trade.
        Returns True if the trade should be deleted.
        """
        # only check on new candles for open entry orders
        if current_time > order.order_date_utc:
            is_entry = order.side == trade.entry_side
            requested_rate = strategy_safe_wrapper(
                self.strategy.adjust_order_price, default_retval=order.ft_price
            )(
                trade=trade,  # type: ignore[arg-type]
                order=order,
                pair=trade.pair,
                current_time=current_time,
                proposed_rate=row[OPEN_IDX],
                current_order_rate=order.ft_price,
                entry_tag=trade.enter_tag,
                side=trade.trade_direction,
                is_entry=is_entry,
            )  # default value is current order price

            # cancel existing order whenever a new rate is requested (or None)
            if requested_rate == order.ft_price:
                # assumption: there can't be multiple open entry orders at any given time
                return False
            else:
                del trade.orders[trade.orders.index(order)]
                if is_entry:
                    self.canceled_entry_orders += 1
                else:
                    self.canceled_exit_orders += 1

            # place new order if result was not None
            if requested_rate:
                if is_entry:
                    self._enter_trade(
                        pair=trade.pair,
                        row=row,
                        trade=trade,
                        requested_rate=requested_rate,
                        requested_stake=(order.safe_remaining * order.ft_price / trade.leverage),
                        direction="short" if trade.is_short else "long",
                    )
                    self.replaced_entry_orders += 1
                else:
                    self._exit_trade(
                        trade=trade,
                        sell_row=row,
                        close_rate=requested_rate,
                        amount=order.safe_remaining,
                        exit_reason=order.ft_order_tag,
                    )
                    self.replaced_exit_orders += 1
                # Delete trade if no successful entries happened (if placing the new order failed)
                if not trade.has_open_orders and is_entry and trade.nr_of_successful_entries == 0:
                    return True
            else:
                # assumption: there can't be multiple open entry orders at any given time
                return trade.nr_of_successful_entries == 0
        return False



    def backtest_loop(
        self,
        row: tuple,
        pair: str,
        current_time: datetime,
        trade_dir: LongShort | None,
        can_enter: bool,
    ) -> LongShort | None:
        """
        NOTE: This method is used by Hyperopt at each iteration. Please keep it optimized.

        Backtesting processing for one candle/pair.
        """
        exiting_dir: LongShort | None = None
        if not self._position_stacking and len(LocalTrade.bt_trades_open_pp[pair]) > 0:
            # position_stacking not supported for now.
            exiting_dir = "short" if LocalTrade.bt_trades_open_pp[pair][0].is_short else "long"

        for t in list(LocalTrade.bt_trades_open_pp[pair]):
            # 1. Manage currently open orders of active trades
            if self.manage_open_orders(t, current_time, row):
                # Remove trade (initial open order never filled)
                LocalTrade.remove_bt_trade(t)
                self.wallets.update()

        # 2. Process entries.
        # without positionstacking, we can only have one open trade per pair.
        # max_open_trades must be respected
        # don't open on the last row
        # We only open trades on the main candle, not on detail candles
        if (
            can_enter
            and trade_dir is not None
            and (self._position_stacking or len(LocalTrade.bt_trades_open_pp[pair]) == 0)
            and not PairLocks.is_pair_locked(pair, row[DATE_IDX], trade_dir)
        ):
            if self.trade_slot_available(LocalTrade.bt_open_open_trade_count):
                trade = self._enter_trade(pair, row, trade_dir)
                if trade:
                    self.wallets.update()
            else:
                self._collate_rejected(pair, row)

        for trade in list(LocalTrade.bt_trades_open_pp[pair]):
            # 3. Process entry orders.
            order = trade.select_order(trade.entry_side, is_open=True)
            if self._try_close_open_order(order, trade, current_time, row):
                self.wallets.update()

            # 4. Create exit orders (if any)
            if trade.has_open_position:
                self._check_trade_exit(trade, row, current_time)  # Place exit order if necessary

            # 5. Process exit orders.
            order = trade.select_order(trade.exit_side, is_open=True)
            if order:
                self._process_exit_order(order, trade, current_time, row, pair)

        if exiting_dir and len(LocalTrade.bt_trades_open_pp[pair]) == 0:
            return exiting_dir
        return None



    def _time_generator_det(self, start_date: datetime, end_date: datetime):
        """
        Loop for each detail candle.
        Yields only the start date if no detail timeframe is set.
        """
        if not self.timeframe_detail_td:
            yield start_date, True, False, 0
            return

        current_time = start_date
        i = 0
        while current_time <= end_date:
            yield current_time, i == 0, True, i
            i += 1
            current_time += self.timeframe_detail_td

    def _time_pair_generator_det(self, current_time: datetime, pairs: list[str]):
        for current_time_det, is_first, has_detail, idx in self._time_generator_det(
            current_time, current_time + self.timeframe_td
        ):
            # Pairs that have open trades should be processed first
            new_pairlist = list(dict.fromkeys([t.pair for t in LocalTrade.bt_trades_open] + pairs))
            for pair in new_pairlist:
                yield current_time_det, is_first, has_detail, idx, pair

    def time_pair_generator(
        self,
        start_date: datetime,
        end_date: datetime,
        pairs: list[str],
        data: dict[str, list[tuple]],
    ):
        """
        Backtest time and pair generator
        :returns: generator of (current_time, pair, row, is_last_row, trade_dir)
            where is_last_row is a boolean indicating if this is the data end date.
        """
        current_time = start_date + self.timeframe_td
        self.progress.init_step(
            BacktestState.BACKTEST, int((end_date - start_date) / self.timeframe_td)
        )
        # Indexes per pair, so some pairs are allowed to have a missing start.
        indexes: dict = defaultdict(int)

        for current_time in self._time_generator(start_date, end_date):
            # Loop for each main candle.
            self.check_abort()
            # Reset open trade count for this candle
            # Critical to avoid exceeding max_open_trades in backtesting
            # when timeframe-detail is used and trades close within the opening candle.
            strategy_safe_wrapper(self.strategy.bot_loop_start, supress_error=True)(
                current_time=current_time
            )
            pair_detail_cache: dict[str, list[tuple]] = {}
            pair_tradedir_cache: dict[str, LongShort | None] = {}
            pairs_with_open_trades = [t.pair for t in LocalTrade.bt_trades_open]

            for current_time_det, is_first, has_detail, idx, pair in self._time_pair_generator_det(
                current_time, pairs
            ):
                # Loop for each detail candle (if necessary) and pair
                # Yields only the main date if no detail timeframe is set.

                # Pairs that have open trades should be processed first
                trade_dir: LongShort | None = None
                if is_first:
                    # Main candle
                    row_index = indexes[pair]
                    row = self.validate_row(data, pair, row_index, current_time)
                    if not row:
                        continue

                    row_index += 1
                    indexes[pair] = row_index
                    is_last_row = current_time == end_date
                    self.dataprovider._set_dataframe_max_index(
                        pair, self.required_startup + row_index
                    )
                    trade_dir = self.check_for_trade_entry(row)
                    pair_tradedir_cache[pair] = trade_dir

                else:
                    # Detail candle - from cache.
                    detail_data = pair_detail_cache.get(pair)
                    if detail_data is None or len(detail_data) <= idx:
                        # logger.info(f"skipping {pair}, {current_time_det}, {trade_dir}")
                        continue
                    row = detail_data[idx]
                    trade_dir = pair_tradedir_cache.get(pair)

                    if self.strategy.ignore_expired_candle(
                        current_time - self.timeframe_td,  # last closed candle is 1 timeframe away.
                        current_time_det,
                        self.timeframe_secs,
                        trade_dir is not None,
                    ):
                        # Ignore late entries eventually
                        trade_dir = None

                self.dataprovider._set_dataframe_max_date(current_time_det)

                pair_has_open_trades = len(LocalTrade.bt_trades_open_pp[pair]) > 0
                if pair in pairs_with_open_trades and not pair_has_open_trades:
                    # Pair has had open trades which closed in the current main candle.
                    # Skip this pair for this timeframe
                    continue
                if pair_has_open_trades and pair not in pairs_with_open_trades:
                    # auto-lock for pairs that have open trades
                    # Necessary for detail - to capture trades that open and close within
                    # the same main candle
                    pairs_with_open_trades.append(pair)

                if (
                    is_first
                    and (trade_dir is not None or pair_has_open_trades)
                    and has_detail
                    and pair not in pair_detail_cache
                    and pair in self.detail_data
                    and row
                ):
                    # Spread candle into detail timeframe and cache that -
                    # only once per main candle
                    # and only if we can expect activity.
                    pair_detail = self.get_detail_data(pair, row)
                    if pair_detail is not None:
                        pair_detail_cache[pair] = pair_detail
                    row = pair_detail_cache[pair][idx]

                is_last_row = current_time_det == end_date

                yield current_time_det, pair, row, is_last_row, trade_dir
            self.progress.increment()

    def backtest(
        self, processed: dict, start_date: datetime, end_date: datetime
    ) -> BacktestContentTypeIcomplete:
        """
        Implement backtesting functionality

        NOTE: This method is used by Hyperopt at each iteration. Please keep it optimized.
        Of course try to not have ugly code. By some accessor are sometime slower than functions.
        Avoid extensive logging in this method and functions it calls.

        :param processed: a processed dictionary with format {pair, data}, which gets cleared to
        optimize memory usage!
        :param start_date: backtesting timerange start datetime
        :param end_date: backtesting timerange end datetime
        :return: DataFrame with trades (results of backtesting)
        """
        self.prepare_backtest(self.enable_protections)
        # Ensure wallets are up-to-date (important for --strategy-list)
        self.wallets.update()
        # Use dict of lists with data for performance
        # (looping lists is a lot faster than pandas DataFrames)
        data: dict = self._get_ohlcv_as_lists(processed)

        # Loop timerange and get candle for each pair at that point in time
        for (
            current_time,
            pair,
            row,
            is_last_row,
            trade_dir,
        ) in self.time_pair_generator(start_date, end_date, list(data.keys()), data):
            if not self._can_short or trade_dir is None:
                # No need to reverse position if shorting is disabled or there's no new signal
                self.backtest_loop(row, pair, current_time, trade_dir, not is_last_row)
            else:
                # Conditionally call backtest_loop a 2nd time if shorting is enabled,
                # a position closed and a new signal in the other direction is available.

                for _ in (0, 1):
                    a = self.backtest_loop(row, pair, current_time, trade_dir, not is_last_row)
                    if not a or a == trade_dir:
                        # the trade didn't close or position change is in the same direction
                        break

        self.handle_left_open(LocalTrade.bt_trades_open_pp, data=data)
        self.wallets.update()

        results = trade_list_to_dataframe(LocalTrade.bt_trades)
        return {
            "results": results,
            "config": self.strategy.config,
            "locks": PairLocks.get_all_locks(),
            "rejected_signals": self.rejected_trades,
            "timedout_entry_orders": self.timedout_entry_orders,
            "timedout_exit_orders": self.timedout_exit_orders,
            "canceled_trade_entries": self.canceled_trade_entries,
            "canceled_entry_orders": self.canceled_entry_orders,
            "replaced_entry_orders": self.replaced_entry_orders,
            "final_balance": self.wallets.get_total(self.strategy.config["stake_currency"]),
        }

    def backtest_one_strategy(
        self, strat: IStrategy, data: dict[str, DataFrame], timerange: TimeRange
    ):
        self.progress.init_step(BacktestState.ANALYZE, 0)
        strategy_name = strat.get_strategy_name()
        logger.info(f"Running backtesting for Strategy {strategy_name}")
        backtest_start_time = dt_now()
        self._set_strategy(strat)

        # need to reprocess data every time to populate signals
        preprocessed = self.strategy.advise_all_indicators(data)

        # Trim startup period from analyzed dataframe
        # This only used to determine if trimming would result in an empty dataframe
        preprocessed_tmp = trim_dataframes(preprocessed, timerange, self.required_startup)

        if not preprocessed_tmp:
            raise OperationalException("No data left after adjusting for startup candles.")

        # Use preprocessed_tmp for date generation (the trimmed dataframe).
        # Backtesting will re-trim the dataframes after entry/exit signal generation.
        min_date, max_date = history.get_timerange(preprocessed_tmp)
        logger.info(
            f"Backtesting with data from {min_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"up to {max_date.strftime(DATETIME_PRINT_FORMAT)} "
            f"({(max_date - min_date).days} days)."
        )
        # Execute backtest and store results
        results = self.backtest(
            processed=preprocessed,
            start_date=min_date,
            end_date=max_date,
        )
        backtest_end_time = dt_now()
        results.update(
            {
                "run_id": self.run_ids.get(strategy_name, ""),
                "backtest_start_time": int(backtest_start_time.timestamp()),
                "backtest_end_time": int(backtest_end_time.timestamp()),
            }
        )
        self.all_bt_content[strategy_name] = results

        if (
            self.config.get("export", "none") == "signals"
            and self.dataprovider.runmode == RunMode.BACKTEST
        ):
            signals = generate_trade_signal_candles(preprocessed_tmp, results, "open_date")
            rejected = generate_rejected_signals(preprocessed_tmp, self.rejected_dict)
            exited = generate_trade_signal_candles(preprocessed_tmp, results, "close_date")

            self.analysis_results["signals"][strategy_name] = signals
            self.analysis_results["rejected"][strategy_name] = rejected
            self.analysis_results["exited"][strategy_name] = exited

        return min_date, max_date

 

    def load_prior_backtest(self):
        self.run_ids = {
            strategy.get_strategy_name(): get_strategy_run_id(strategy)
            for strategy in self.strategylist
        }

        # Load previous result that will be updated incrementally.
        # This can be circumvented in certain instances in combination with downloading more data
        min_backtest_date = self._get_min_cached_backtest_date()
        if min_backtest_date is not None:
            self.results = find_existing_backtest_stats(
                self.config["user_data_dir"] / "backtest_results", self.run_ids, min_backtest_date
            )

    def start(self) -> None:
        """
        Run backtesting end-to-end
        """
        data: dict[str, DataFrame] = {}

        data, timerange = self.load_bt_data()
        logger.info("Dataload complete. Calculating indicators")

        self.load_prior_backtest()

        for strat in self.strategylist:
            if self.results and strat.get_strategy_name() in self.results["strategy"]:
                # When previous result hash matches - reuse that result and skip backtesting.
                logger.info(f"Reusing result of previous backtest for {strat.get_strategy_name()}")
                continue
            min_date, max_date = self.backtest_one_strategy(strat, data, timerange)

        # Update old results with new ones.
        if len(self.all_bt_content) > 0:
            results = generate_backtest_stats(
                data,
                self.all_bt_content,
                min_date=min_date,
                max_date=max_date,
                notes=self.config.get("backtest_notes"),
            )
            if self.results:
                self.results["metadata"].update(results["metadata"])
                self.results["strategy"].update(results["strategy"])
                self.results["strategy_comparison"].extend(results["strategy_comparison"])
            else:
                self.results = results
            dt_appendix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            if self.config.get("export", "none") in ("trades", "signals"):
                combined_res = combined_dataframes_with_rel_mean(data, min_date, max_date)
                store_backtest_results(
                    self.config,
                    self.results,
                    dt_appendix,
                    market_change_data=combined_res,
                    analysis_results=self.analysis_results,
                    strategy_files={s.get_strategy_name(): s.__file__ for s in self.strategylist},
                )

        # Results may be mixed up now. Sort them so they follow --strategy-list order.
        if "strategy_list" in self.config and len(self.results) > 0:
            self.results["strategy_comparison"] = sorted(
                self.results["strategy_comparison"],
                key=lambda c: self.config["strategy_list"].index(c["key"]),
            )
            self.results["strategy"] = dict(
                sorted(
                    self.results["strategy"].items(),
                    key=lambda kv: self.config["strategy_list"].index(kv[0]),
                )
            )

        if len(self.strategylist) > 0:
            # Show backtest results
            show_backtest_results(self.config, self.results)
