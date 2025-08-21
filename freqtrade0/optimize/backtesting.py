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
import freqtrade.optimize
from freqtrade.optimize.backtest_caching import get_strategy_run_id
import freqtrade.optimize.backtest_caching
import freqtrade.optimize.backtesting
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
from freqtrade.persistence.trade_model import ProfitStruct
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

backtestclass= freqtrade.optimize.backtesting.Backtesting
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

    def _check_adjust_trade_position( self, trade: LocalTrade, row: tuple, current_time: datetime):
            """
            Check the implemented trading strategy for adjustment command.
            If the strategy triggers the adjustment, a new order gets issued.
            Once that completes, the existing trade is modified to match new data.
            """
            current_entry_rate, current_exit_rate = self.exchange.get_rates(
                trade.pair, True, trade.is_short
            )
            current_entry_profit_struc: ProfitStruct = ProfitStruct(0,0,0,0)
            match len(trade.select_filled_orders()) :
                case l if l < 1:
                    current_profit= 0
                    current_exit_profit = 0
                case _:
                    current_profit = trade.calc_profit_ratio(current_entry_rate)
                    current_exit_profit = trade.calc_profit_ratio(current_exit_rate)
                    current_entry_profit_struc: ProfitStruct = trade.calculate_profit(current_entry_rate)
            # i fix this calc_profit

            min_entry_stake = self.exchange.get_min_pair_stake_amount(
                trade.pair, current_entry_rate, 0.0, trade.leverage
            )
            min_exit_stake = self.exchange.get_min_pair_stake_amount(
                trade.pair, current_exit_rate, self.strategy.stoploss, trade.leverage
            )
            max_entry_stake = self.exchange.get_max_pair_stake_amount(
                trade.pair, current_entry_rate, trade.leverage
            )
            stake_available = self.wallets.get_available_stake_amount()
            self.log_once(f"Calling adjust_trade_position for pair {trade.pair}",logger.info)
            orders = self.strategy._adjust_trade_position_internal(
                trade=trade,
                current_time=datetime.now(UTC),
                current_rate=current_entry_rate,
                current_profit=current_profit,
                min_stake=min_entry_stake,
                max_stake=min(max_entry_stake, stake_available),
                current_entry_rate=current_entry_rate,
                current_exit_rate=current_exit_rate,
                current_entry_profit=current_profit,
                current_exit_profit=current_exit_profit,profit_struc=current_entry_profit_struc
            )
    def _get_exit_for_signal(
        self,
        trade: LocalTrade,
        row: tuple,
        exit_: ExitCheckTuple,
        current_time: datetime,
        amount: float | None = None,
    ) -> LocalTrade | None:
        if exit_.exit_flag:
            trade.close_date = current_time
            exit_reason = exit_.exit_reason
            amount_ = amount if amount is not None else trade.amount
            trade_dur = int((trade.close_date_utc - trade.open_date_utc).total_seconds() // 60)
            try:
                close_rate = self._get_close_rate(row, trade, current_time, exit_, trade_dur)
            except ValueError:
                return None
            # call the custom exit price,with default value as previous close_rate
            current_profit = trade.calc_profit_ratio(close_rate)
            order_type = self.strategy.order_types["exit"]
            if exit_.exit_type in (
                ExitType.EXIT_SIGNAL,
                ExitType.CUSTOM_EXIT,
                ExitType.PARTIAL_EXIT,
            ):
                # Checks and adds an exit tag, after checking that the length of the
                # row has the length for an exit tag column
                if (
                    len(row) > EXIT_TAG_IDX
                    and row[EXIT_TAG_IDX] is not None
                    and len(row[EXIT_TAG_IDX]) > 0
                    and exit_.exit_type in (ExitType.EXIT_SIGNAL,)
                ):
                    exit_reason = row[EXIT_TAG_IDX]
                # Custom exit pricing only for exit-signals
                if order_type == "limit":
                    rate = strategy_safe_wrapper(
                        self.strategy.custom_exit_price, default_retval=close_rate
                    )(
                        pair=trade.pair,
                        trade=trade,  # type: ignore[arg-type]
                        current_time=current_time,
                        proposed_rate=close_rate,
                        current_profit=current_profit,
                        exit_tag=exit_reason,
                    )
                    if rate is not None and rate != close_rate:
                        close_rate = price_to_precision(
                            rate, trade.price_precision, trade.precision_mode_price
                        )
                    # We can't place orders lower than current low.
                    # freqtrade does not support this in live, and the order would fill immediately
                    if trade.is_short:
                        close_rate = min(close_rate, row[HIGH_IDX])
                    else:
                        close_rate = max(close_rate, row[LOW_IDX])
            # Confirm trade exit:
            time_in_force = self.strategy.order_time_in_force["exit"]

            if exit_.exit_type not in (
                ExitType.LIQUIDATION,
                ExitType.PARTIAL_EXIT,
            ) and not strategy_safe_wrapper(self.strategy.confirm_trade_exit, default_retval=True)(
                pair=trade.pair,
                trade=trade,  # type: ignore[arg-type]
                order_type=order_type,
                amount=amount_,
                rate=close_rate,
                time_in_force=time_in_force,
                sell_reason=exit_reason,  # deprecated
                exit_reason=exit_reason,
                current_time=current_time,
            ):
                return None

            trade.exit_reason = exit_reason

            return self._exit_trade(trade, row, close_rate, amount_, exit_reason)
        return None
    #TODO
    def _check_adjust_trade_for_candle(
        self, trade: LocalTrade, row: tuple, current_time: datetime
    ) -> LocalTrade:
        '''
        需要与freqtrade 中的check_and_call_adjust_trade_position方法一致
        '''
        current_rate: float = row[OPEN_IDX]
        # current_profit = trade.calc_profit_ratio(current_rate)
        current_entry_profit_struc: ProfitStruct = ProfitStruct(0,0,0,0)
        match len(trade.select_filled_orders()) :
            case l if l < 1:
                current_profit= 0
            case _:
                current_profit = trade.calc_profit_ratio(current_rate)
                current_entry_profit_struc: ProfitStruct = trade.calculate_profit(current_rate)
        min_stake = self.exchange.get_min_pair_stake_amount(trade.pair, current_rate, -0.1)
        max_stake = self.exchange.get_max_pair_stake_amount(trade.pair, current_rate)
        stake_available = self.wallets.get_available_stake_amount()
        
        orders = self.strategy._adjust_trade_position_internal(
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
        for stake_amount, price,order_tag in orders:
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
                        requested_rate=price,
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
        for stake_amount, price,order_tag in orders:
               

                    self.execute_entry(
                        trade.pair,
                        stake_amount=stake_amount,
                        price=price,
                        trade=trade,
                        is_short=trade.is_short,
                        mode="pos_adjust",
                        enter_tag=order_tag,
                    )

                if stake_amount is not None and stake_amount < 0.0:
                    # We should decrease our position
                    amount = self.exchange.amount_to_contract_precision(
                        trade.pair,
                        abs(
                            float(
                                FtPrecise(stake_amount)
                                * FtPrecise(trade.amount)
                                / FtPrecise(trade.stake_amount)
                            )
                        ),
                    )

                    if amount == 0.0:
                        logger.info(
                            f"Wanted to exit of {stake_amount} amount, "
                            "but exit amount is now 0.0 due to exchange limits - not exiting."
                        )
                        continue

                    remaining = (trade.amount - amount) * price
                    if min_exit_stake and remaining != 0 and remaining < min_exit_stake:
                        logger.info(
                            f"Remaining amount of {remaining} would be smaller "
                            f"than the minimum of {min_exit_stake}."
                        )
                        continue

                    self.execute_trade_exit(
                        trade,
                        price,
                        exit_check=ExitCheckTuple(exit_type=ExitType.PARTIAL_EXIT),
                        sub_trade_amt=amount,
                        exit_tag=order_tag,
                    )
        return trade







