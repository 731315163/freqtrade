"""
Freqtrade is the main module of this bot. It contains the class Freqtrade()
"""

import asyncio
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from threading import Lock
from typing import cast
from unittest import case

import jsonschema
import pandas as pd
from pandas import DataFrame
from schedule import Scheduler

from freqtrade.exchange.exchange_types import CcxtOrder
import freqtrade.freqtradebot
from freqtrade import constants
from freqtrade.configuration import validate_config_consistency
from freqtrade.constants import Config, ExchangeConfig
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import (
    ExitCheckTuple,
    ExitType,
    MarginMode,
    SignalDirection,
    State,
    TradingMode,
)
from freqtrade.exceptions import DependencyException, InsufficientFundsError
from freqtrade.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange import Exchange
from freqtrade.mixins import LoggingMixin
from freqtrade.persistence import PairLocks, Trade, init_db
from freqtrade.persistence.models import PairLock
from freqtrade.persistence.trade_model import Order, ProfitStruct
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.rpc import RPCManager
from freqtrade.rpc.external_message_consumer import ExternalMessageConsumer
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, MeasureTime, PeriodicCache, dt_now
from freqtrade.wallets import Wallets
from freqtrade0.enums import TradeDirection
from freqtrade0.resolvers import ExchangeResolver, StrategyResolver
from freqtrade0.strategy import IStrategy


logger = freqtrade.freqtradebot.logger
class FreqtradeBot(freqtrade.freqtradebot.FreqtradeBot):
    """
    Freqtrade is the main class of the bot.
    This is from here the bot start its logic.
    """
    def __init__(self, config: Config,strategy_type:type|None=None) -> None:
        """
        Init all variables and objects the bot needs to work
        :param config: configuration dict, you can use Configuration.get_config()
        to get the config dict.
        """
        self.log_cache:dict[str,datetime] = {}
        self.refresh_period = 60
        self.active_pair_whitelist: list[str] = []

        # Init bot state
        self.state = State.STOPPED

        # Init objects
        self.config = config
        exchange_config: ExchangeConfig = deepcopy(config["exchange"])
        # Remove credentials from original exchange config to avoid accidental credential exposure
        if strategy_type:
            self.strategy :IStrategy= StrategyResolver.create_strategy(strategy_type=strategy_type,config=self.config)
        else:
            self.strategy :IStrategy=cast(IStrategy, StrategyResolver.load_strategy(self.config))
        self.strategy.bot = self
        # Check config consistency here since strategies can set certain options
        try:
            validate_config_consistency(config)
        except jsonschema.ValidationError as e :
            logger.error(e)



        self.exchange: Exchange = ExchangeResolver.load_exchange(
            self.config, exchange_config=exchange_config, load_leverage_tiers=True
        )
        init_db(self.config["db_url"])

        self.wallets = Wallets(self.config, self.exchange)

        PairLocks.timeframe = self.config["timeframe"]

        self.trading_mode: TradingMode = self.config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = self.config.get("margin_mode", MarginMode.NONE)
        self.last_process: datetime | None = None

        # RPC runs in separate threads, can start handling external commands just after
        # initialization, even before Freqtradebot has a chance to start its throttling,
        # so anything in the Freqtradebot instance should be ready (initialized), including
        # the initial state of the bot.
        # Keep this at the end of this initialization method.
        self.rpc: RPCManager = RPCManager(self)

        self.dataprovider = DataProvider(self.config, self.exchange, rpc=self.rpc)
        self.pairlists = PairListManager(self.exchange, self.config, self.dataprovider)

        self.dataprovider.add_pairlisthandler(self.pairlists)

        # Attach Dataprovider to strategy instance
        self.strategy.dp = self.dataprovider
        # Attach Wallets to strategy instance
        self.strategy.wallets = self.wallets

      
        # Init ExternalMessageConsumer if enabled
        self.emc = (
            ExternalMessageConsumer(self.config, self.dataprovider)
            if self.config.get("external_message_consumer", {}).get("enabled", False)
            else None
        )

        logger.info("Starting initial pairlist refresh")
        with MeasureTime(
            lambda duration, _: logger.info(f"Initial Pairlist refresh took {duration:.2f}s"), 0
        ):
            self.active_pair_whitelist = self._refresh_active_whitelist()

        # Set initial bot state from config
        initial_state:str =cast(str, self.config.get("initial_state"))
        self.state = State[initial_state.upper()] if initial_state else State.STOPPED

        # Protect exit-logic from forcesell and vice versa
        self._exit_lock = Lock()
        timeframe_secs = timeframe_to_seconds(self.strategy.timeframe)
        self._exit_reason_cache = PeriodicCache(100, ttl=timeframe_secs)
        LoggingMixin.__init__(self, logger, timeframe_secs)

        self._schedule = Scheduler()

        if self.trading_mode == TradingMode.FUTURES:

            def update():
                self.update_funding_fees()
                self.update_all_liquidation_prices()
                self.wallets.update()

            # This would be more efficient if scheduled in utc time, and performed at each
            # funding interval, specified by funding_fee_times on the exchange classes
            # However, this reduces the precision - and might therefore lead to problems.
            for time_slot in range(0, 24):
                for minutes in [1, 31]:
                    t = str(time(time_slot, minutes, 2))
                    self._schedule.every().day.at(t).do(update)

        self._schedule.every().day.at("00:02").do(self.exchange.ws_connection_reset)

        self.strategy.ft_bot_start()
        # Initialize protections AFTER bot start - otherwise parameters are not loaded.
        self.protections = ProtectionManager(self.config, self.strategy.protections)
        self.use_public_trades = self.config.get("exchange", {}).get("use_public_trades", False)
        if not self.use_public_trades:
            logger.info("Using public trades is disabled. ")
        def log_took_too_long(duration: float, time_limit: float):
            logger.warning(
                f"Strategy analysis took {duration:.2f}s, more than 25% of the timeframe "
                f"({time_limit:.2f}s). This can lead to delayed orders and missed signals."
                "Consider either reducing the amount of work your strategy performs "
                "or reduce the amount of pairs in the Pairlist."
            )

        self._measure_execution = MeasureTime(log_took_too_long, timeframe_secs * 0.25)


 
    # def log_once(self, message: str, logmethod: Callable, force_show: bool = False) -> None:
    #     """
    #     Logs message - not more often than "refresh_period" to avoid log spamming
    #     Logs the log-message as debug as well to simplify debugging.
    #     :param message: String containing the message to be sent to the function.
    #     :param logmethod: Function that'll be called. Most likely `logger.info`.
    #     :param force_show: If True, sends the message regardless of show_output value.
    #     :return: None.
    #     """
    #     now_time = dt_now()
    #     internal = timedelta(seconds=self.refresh_period)
    #     delkeys = []
    #     for k  ,v in self.log_cache.items():
    #         if now_time - v > internal:
    #             delkeys.append(k)
    #     for k in delkeys:
    #         del self.log_cache[k]
    #     if message not in self.log_cache:
    #         logmethod(message)
    #         self.log_cache[message] = now_time

    def _getpairlist(self,informative_pairlist):
        trades: list[Trade] = Trade.get_open_trades()
        self.active_pair_whitelist = self._refresh_active_whitelist(trades)
        pairlist = self.pairlists.create_pair_list(self.active_pair_whitelist)
        res_pair_list = pairlist + informative_pairlist if informative_pairlist else pairlist
        return res_pair_list


    def _get_nolock_whitelist(self,can_hedge_mode: bool=False) -> dict[str,TradeDirection]:
        """
        获取非锁定状态下的白名单 若存在全局锁定则返回空列表或 None。
        """
        # 创建白名单的深拷贝
        whitelist = self.active_pair_whitelist

        # 如果白名单为空 记录日志并返回
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
            return {}

        def del_pair(pair:str,side:str,tradepairs:dict[str,TradeDirection]):
            if pair not in tradepairs:
                return False
            tradepairs[pair] = tradepairs[pair] |  TradeDirection.convert(side)

            if pair == "*":
                return False
            can_hedge_mode_condtion =(not can_hedge_mode and tradepairs[pair] > TradeDirection.NONE)
            if tradepairs[pair] == TradeDirection.BOTH or  can_hedge_mode_condtion:
                del tradepairs[pair]
                return True
            return False

        tradepairs:dict[str,TradeDirection] = { pair:TradeDirection.NONE for pair in whitelist}
        tradepairs["*"] = TradeDirection.NONE
        for pairlock in PairLocks.get_pair_locks(pair=None):
            pairlock = cast(PairLock, pairlock)
            is_del = del_pair(pairlock.pair,pairlock.side,tradepairs)
            if is_del:
              self.log_once(
                rf"Pair {pairlock.pair} {pairlock.side} is locked. datetime until {pairlock.lock_end_time}",
                logger.info,
            )

        global_lock_side = tradepairs.pop("*")
        if  global_lock_side == TradeDirection.BOTH:
                self.log_once("Global pairlock active. Not creating new trades.", logger.info)
                return {}
        elif global_lock_side != TradeDirection.NONE:
                for k ,v in tradepairs.items():
                    tradepairs[k] = v | global_lock_side


        for trade in Trade.get_open_trades():
            trade = cast(Trade, trade)
            del_pair(trade.pair,side=trade.trade_direction,tradepairs=tradepairs)
        if len(tradepairs) <= 0:
            self.log_once(
                "No currency pair in active pair whitelist, but checking to exit open trades.",
                logger.info,
            )
        return tradepairs

    #
    # enter positions / open trades logic and methods
    #
 


    def create_trade(self, pair: str,*,notrade_direction:TradeDirection=TradeDirection.NONE,df:DataFrame|None =None):
        """
        修改为双向持仓逻辑
        Check the implemented trading strategy for entry signals.

        If the pair triggers the enter signal a new trade record gets created
        and the entry-order opening the trade gets issued towards the exchange.

        :return: True if a trade has been created.
        """
        
        if df is not None and not df.empty :
            analyzed_df = df
        
        else:
            analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair, self.strategy.timeframe)
        # nowtime = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None
           # try:
        #交易次数
        num = 0
        # running get_signal on historical data fetched
        #价格或交易都可以直接返回none
        signals, enter_tag = self.strategy.get_entry_signal(
            pair, self.strategy.timeframe, analyzed_df
        )
        if  signals is  None :
            return num
        signals = (TradeDirection.BOTH ^ notrade_direction)&signals
        if signals==TradeDirection.NONE :
            return num
        stake_amount = self.wallets.get_trade_stake_amount(
            pair, self.config["max_open_trades"]
        )
        bid_check_dom = self.config.get("entry_pricing", {}).get("check_depth_of_market", {})
        def _execute_entry(signal,enter_tag:str|None):
            if (bid_check_dom.get("enabled", False)) and (bid_check_dom.get("bids_to_ask_delta", 0) > 0):
                if self._check_depth_of_market(pair, bid_check_dom, side=signal):
                    if self.execute_entry(
                        pair,
                        stake_amount=stake_amount,
                        enter_tag=enter_tag,
                        is_short=(signal == SignalDirection.SHORT),
                    ):
                        return 1
            else:

                if self.execute_entry(
                    pair=pair, stake_amount=stake_amount ,enter_tag=enter_tag, is_short=(signal == SignalDirection.SHORT)
                ):
                    return 1
            return 0
        
        match signals:
            case TradeDirection.BOTH:
                num+= _execute_entry(SignalDirection.LONG,enter_tag[0])
                num+= _execute_entry(SignalDirection.SHORT,enter_tag[1])
            case TradeDirection.LONG:
                num+= _execute_entry(SignalDirection.LONG,enter_tag)
            case TradeDirection.SHORT:
                num+= _execute_entry(SignalDirection.SHORT,enter_tag)
            case _:
                raise ValueError(f"Invalid trade direction: {signals}")
        return num


    def enter_positions(self) -> int:
        whitelist = self._get_nolock_whitelist(can_hedge_mode=True)
        trades_created = 0
        trades_created_ohlc = 0
        refrence_ohlc = {}
        if len(whitelist) > 0:
            for pair,direction in whitelist.items():

                try:
                    analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair=pair, timeframe=self.strategy.timeframe)
                    if not analyzed_df.empty and "date" in analyzed_df.columns:
                        latest_time=analyzed_df["date"].iloc[-1]
                        pair_list = refrence_ohlc.get(latest_time,[])
                        pair_list.append(pair)
                        refrence_ohlc[latest_time]=pair_list
                    else:
                        self.log_once(f"Found no ohlc data for {pair},unable to create trade.", logger.debug)

                    if  self.get_free_open_trades() <= 0:
                        self.log_once(f"not opening new trade for {pair},free open trades is less than 0.", logger.info)
                        break

                    with self._exit_lock:
                            trades_created_ohlc += self.create_trade(pair,notrade_direction= direction,df=analyzed_df)
                except DependencyException as exception:
                    logger.warning("Unable to create trade for %s: %s", pair, exception)

            msg=""
            for t,p in refrence_ohlc.items():
                p_str = ",".join(p)
                msg+=f"{t}:{p_str}\n"
            if (trades_created+trades_created_ohlc)==0:
                logger.info( "Found no enter signals for whitelisted currencies.")
            else:
                self.log_once(f"refresh data {msg}...", logger.info)
        return trades_created+trades_created_ohlc
    def check_and_call_adjust_trade_position(self, trade: Trade):
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
            #   cancel open orders of this trade if order is different
            self.cancel_open_orders_of_trade(
                trade,
                [trade.entry_side, trade.exit_side],
                constants.CANCEL_REASON["REPLACE"],
                True,
            )
            Trade.commit()
            for stake_amount, price,order_tag in orders:
                if stake_amount is not None and stake_amount > 0.0:
                    if self.state == State.PAUSED:
                        logger.debug("Position adjustment aborted because the bot is in PAUSED state")
                        continue

                    # We should increase our position
                    if self.strategy.max_entry_position_adjustment > -1:
                        count_of_entries = trade.nr_of_successful_entries
                        if count_of_entries > self.strategy.max_entry_position_adjustment:
                            logger.debug(f"Max adjustment entries for {trade.pair} has been reached.")
                            continue
                        else:
                            logger.debug("Max adjustment entries is set to unlimited.")

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
 
    def handle_similar_open_order(
        self, trade: Trade, price: float, amount: float, side: str
    ) -> bool:
        """
        Keep existing open order if same amount and side otherwise cancel
        :param trade: Trade object of the trade we're analyzing
        :param price: Limit price of the potential new order
        :param amount: Quantity of assets of the potential new order
        :param side: Side of the potential new order
        :return: True if an existing similar order was found
        """
        if trade.has_open_orders:
            oo = trade.select_order(side, True)
            if oo is not None:
                if price == oo.price and side == oo.side and amount == oo.amount:
                    logger.info(
                        f"A similar open order was found for {trade.pair}. "
                        f"Keeping existing {trade.exit_side} order. {price=},  {amount=}"
                    )
                    return True
            # cancel open orders of this trade if order is different
            # self.cancel_open_orders_of_trade(
            #     trade,
            #     [trade.entry_side, trade.exit_side],
            #     constants.CANCEL_REASON["REPLACE"],
            #     True,
            # )
            # Trade.commit()
            return False

        return False
    def handle_cancel_order(
        self, order: CcxtOrder, order_obj: Order, trade: Trade, reason: str, replacing: bool = False
    ) -> bool:
        """
        Check if current analyzed order timed out and cancel if necessary.
        :param order: Order dict grabbed with exchange.fetch_order()
        :param order_obj: Order object from the database.
        :param trade: Trade object.
        :return: True if the order was canceled, False otherwise.
        """
        if order["side"] == trade.entry_side:
            return self.handle_cancel_enter(trade, order, order_obj, reason, replacing)
        else:
            canceled = self.handle_cancel_exit(trade, order, order_obj, reason)
            # if not replacing:
            #     canceled_count = trade.get_canceled_exit_order_count()
            #     max_timeouts = self.config.get("unfilledtimeout", {}).get("exit_timeout_count", 0)
            #     if canceled and max_timeouts > 0 and canceled_count >= max_timeouts:
            #         logger.warning(
            #             f"Emergency exiting trade {trade}, as the exit order "
            #             f"timed out {max_timeouts} times. force selling {order['amount']}."
            #         )
            #         self.emergency_exit(trade, order["price"], order["amount"])
            return canceled