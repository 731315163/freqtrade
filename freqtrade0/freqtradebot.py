"""
Freqtrade is the main module of this bot. It contains the class Freqtrade()
"""

import logging
from copy import deepcopy
from datetime import datetime, time, timezone
from threading import Lock
from typing import cast

import jsonschema
from pandas import DataFrame
from schedule import Scheduler

from freqtrade import constants
from freqtrade0.data.dataprovider import DataProvider
from freqtrade0.exchange import (
    amount_to_contract_precision,
    price_to_precision,
    remove_exchange_credentials,
    timeframe_to_seconds,
)
from freqtrade0.resolvers import ExchangeResolver, StrategyResolver
from freqtrade0.strategy import IStrategy

from freqtrade.configuration import validate_config_consistency
from freqtrade.constants import Config, ExchangeConfig
from freqtrade.edge import Edge
from freqtrade.enums import (
    ExitCheckTuple,
    ExitType,
    MarginMode,
    SignalDirection,
    State,
    TradingMode,
)
from freqtrade.exceptions import (
    DependencyException,
)
from freqtrade.mixins import LoggingMixin
from freqtrade.persistence import PairLocks, Trade, init_db
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.rpc import RPCManager
from freqtrade.rpc.external_message_consumer import ExternalMessageConsumer
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, MeasureTime, PeriodicCache, dt_now
from freqtrade.wallets import Wallets


logger = logging.getLogger(__name__)

import freqtrade.freqtradebot


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
        self.active_pair_whitelist: list[str] = []

        # Init bot state
        self.state = State.STOPPED

        # Init objects
        self.config = config
        exchange_config: ExchangeConfig = deepcopy(config["exchange"])
        # Remove credentials from original exchange config to avoid accidental credential exposure
        remove_exchange_credentials(config["exchange"], True)
        if strategy_type:
            self.strategy :IStrategy= StrategyResolver.create_strategy(strategy_type=strategy_type,config=self.config)
        else:
            self.strategy :IStrategy=cast(IStrategy,  StrategyResolver.load_strategy(self.config))

        # Check config consistency here since strategies can set certain options
        try:
            validate_config_consistency(config)
        except jsonschema.ValidationError as e :
            logger.error(e)

        self.exchange = ExchangeResolver.load_exchange(
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

        # Initializing Edge only if enabled
        self.edge = (
            Edge(self.config, self.exchange, self.strategy)
            if self.config.get("edge", {}).get("enabled", False)
            else None
        )

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

        def log_took_too_long(duration: float, time_limit: float):
            logger.warning(
                f"Strategy analysis took {duration:.2f}s, more than 25% of the timeframe "
                f"({time_limit:.2f}s). This can lead to delayed orders and missed signals."
                "Consider either reducing the amount of work your strategy performs "
                "or reduce the amount of pairs in the Pairlist."
            )

        self._measure_execution = MeasureTime(log_took_too_long, timeframe_secs * 0.25)
        
        
        
    def process(self) -> None:
        """
        Queries the persistence layer for open trades and handles them,
        otherwise a new trade is created.
        :return: True if one or more trades has been created or closed, False otherwise
        """

        # Check whether markets have to be reloaded and reload them when it's needed
        self.exchange.reload_markets()

        self.update_trades_without_assigned_fees()

        # Query trades from persistence layer
        trades: list[Trade] = Trade.get_open_trades()

        self.active_pair_whitelist = self._refresh_active_whitelist(trades)
        pairlist = self.pairlists.create_pair_list(self.active_pair_whitelist)
        ohlcv_pair_list =self.dataprovider.merge_pairs_helperpairs(pairlist,
            self.strategy.gather_informative_pairs())
        self.dataprovider.refresh_latest_ohlcv(ohlcv_pair_list)
        trade_pair_list = self.dataprovider.merge_pairs_helperpairs(pairlist,self.strategy.gather_informative_trade_pairs())
        self.dataprovider.refresh_latest_trades(trade_pair_list)
        strategy_safe_wrapper(self.strategy.bot_loop_start, supress_error=True)(
            current_time=datetime.now(timezone.utc)
        )

        with self._measure_execution:
                self.strategy.analyze(self.active_pair_whitelist)

        with self._exit_lock:
            # Check for exchange cancellations, timeouts and user requested replace
            self.manage_open_orders()

        # Protect from collisions with force_exit.
        # Without this, freqtrade may try to recreate stoploss_on_exchange orders
        # while exiting is in process, since telegram messages arrive in an different thread.
        with self._exit_lock:
            trades = Trade.get_open_trades()
            # First process current opened trades (positions)
            self.exit_positions(trades)
            Trade.commit()

        # Check if we need to adjust our current positions before attempting to enter new trades.
        if self.strategy.position_adjustment_enable:
            with self._exit_lock:
                    self.process_open_trade_positions()

        # Then looking for entry opportunities
        if self.state == State.RUNNING and self.get_free_open_trades():
            self.enter_positions()
               
        self._schedule.run_pending()
        Trade.commit()
        self.rpc.process_msg_queue(self.dataprovider._msg_queue)
        self.last_process = datetime.now(timezone.utc)
    def process_open_trade_positions(self):
        """
        Tries to execute additional buy or sell orders for open trades (positions)
        """
        # Walk through each pair and check if it needs changes
        for trade in Trade.get_open_trades():
            # If there is any open orders, wait for them to finish.
            # TODO Remove to allow mul open orders
            if trade.has_open_position or trade.has_open_orders:
                # Do a wallets update (will be ratelimited to once per hour)
                self.wallets.update(False)
                try:
                    self.check_and_call_adjust_trade_position(trade)
                except DependencyException as exception:
                    logger.warning(
                        f"Unable to adjust position of trade for {trade.pair}: {exception}"
                    )

    def check_and_call_adjust_trade_position(self, trade: Trade):
        """
        Check the implemented trading strategy for adjustment command.
        If the strategy triggers the adjustment, a new order gets issued.
        Once that completes, the existing trade is modified to match new data.
        """
        current_entry_rate, current_exit_rate = self.exchange.get_rates(
            trade.pair, True, trade.is_short
        )
        # i fix this calc_profit
        current_entry_profit = trade.calc_profit_ratio(current_entry_rate)
        current_exit_profit = trade.calc_profit_ratio(current_exit_rate)

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
        logger.debug(f"Calling adjust_trade_position for pair {trade.pair}")
        stake_amount, order_tag = self.strategy._adjust_trade_position_internal(
            trade=trade,
            current_time=datetime.now(timezone.utc),
            current_rate=current_entry_rate,
            current_profit=current_entry_profit,
            min_stake=min_entry_stake,
            max_stake=min(max_entry_stake, stake_available),
            current_entry_rate=current_entry_rate,
            current_exit_rate=current_exit_rate,
            current_entry_profit=current_entry_profit,
            current_exit_profit=current_exit_profit,
        )

        if stake_amount is not None and stake_amount > 0.0:
            if self.state == State.PAUSED:
                logger.debug("Position adjustment aborted because the bot is in PAUSED state")
                return

            # We should increase our position
            if self.strategy.max_entry_position_adjustment > -1:
                count_of_entries = trade.nr_of_successful_entries
                if count_of_entries > self.strategy.max_entry_position_adjustment:
                    logger.debug(f"Max adjustment entries for {trade.pair} has been reached.")
                    return
                else:
                    logger.debug("Max adjustment entries is set to unlimited.")

            self.execute_entry(
                trade.pair,
                stake_amount,
                price=current_entry_rate,
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
                return

            remaining = (trade.amount - amount) * current_exit_rate
            if min_exit_stake and remaining != 0 and remaining < min_exit_stake:
                logger.info(
                    f"Remaining amount of {remaining} would be smaller "
                    f"than the minimum of {min_exit_stake}."
                )
                return

            self.execute_trade_exit(
                trade,
                current_exit_rate,
                exit_check=ExitCheckTuple(exit_type=ExitType.PARTIAL_EXIT),
                sub_trade_amt=amount,
                exit_tag=order_tag,
            )
    def calc_profit_ratio(self, trade: Trade,entry_rate: float, exit_rate: float|None = None):
        
        match len(trade.orders) :
            case l if l < 1:
                current_entry_profit= 0
                current_exit_profit = 0
            case 1:
                current_entry_profit = trade.calc_profit_ratio(entry_rate)
                if exit_rate is None:
                    current_exit_profit = current_entry_profit
                else:
                    current_exit_profit = trade.calc_profit_ratio(exit_rate)
            case _:
                current_entry_profit = self.calc_trade_from_orders(trade)
                if exit_rate is None:
                    current_exit_profit = current_entry_profit
                else:
                    current_exit_profit = trade.calc_profit_ratio(exit_rate)
                    
        return current_entry_profit,current_exit_profit
    def calc_trade_from_orders(self,trade:Trade,rate:float, is_closing: bool = False):

        ZERO = FtPrecise(0.0)
        current_amount = FtPrecise(0.0)
        current_stake = FtPrecise(0.0)
        max_stake_amount = FtPrecise(0.0)
        total_stake = 0.0  # Total stake after all buy orders (does not subtract!)
        avg_price = FtPrecise(0.0)
        close_profit = 0.0
        close_profit_abs = 0.0
        # Reset funding fees
        trade.funding_fees = 0.0
        funding_fees = 0.0
        ordercount = len(trade.orders) - 1
        for i, o in enumerate(trade.orders):
            if o.ft_is_open or not o.filled:
                continue
            funding_fees += o.funding_fee or 0.0
            tmp_amount = FtPrecise(o.safe_amount_after_fee)
            tmp_price = FtPrecise(o.safe_price)

            is_exit = o.ft_order_side != trade.entry_side
            side = FtPrecise(-1 if is_exit else 1)
            if tmp_amount > ZERO and tmp_price is not None:
                current_amount += tmp_amount * side
                price = avg_price if is_exit else tmp_price
                current_stake += price * tmp_amount * side

                if current_amount > ZERO and not is_exit:
                    avg_price = current_stake / current_amount

            if is_exit:
                # Process exits
                if i == ordercount and is_closing:
                    # Apply funding fees only to the last closing order
                    trade.funding_fees = funding_fees

                exit_rate = o.safe_price
                exit_amount = o.safe_amount_after_fee
                prof = trade.calculate_profit(exit_rate, exit_amount, float(avg_price))
                close_profit_abs += prof.profit_abs
                if total_stake > 0:
                    # This needs to be calculated based on the last occurring exit to be aligned
                    # with realized_profit.
                    close_profit = (close_profit_abs / total_stake) * trade.leverage
            else:
                total_stake += trade._calc_open_trade_value(tmp_amount, price)
                max_stake_amount += tmp_amount * price
        trade.funding_fees = funding_fees
        trade.max_stake_amount = float(max_stake_amount) / (trade.leverage or 1.0)

        if close_profit:
            trade.close_profit = close_profit
            trade.realized_profit = close_profit_abs
            trade.close_profit_abs = prof.profit_abs

        current_amount_tr = amount_to_contract_precision(
            float(current_amount), trade.amount_precision, trade.precision_mode, trade.contract_size
        )
        if current_amount_tr > 0.0:
            # Trade is still open
            # Leverage not updated, as we don't allow changing leverage through DCA at the moment.
            trade.open_rate = price_to_precision(
                float(current_stake / current_amount),
                trade.price_precision,
                trade.precision_mode_price,
            )
            trade.amount = current_amount_tr
            trade.stake_amount = float(current_stake) / (trade.leverage or 1.0)
            trade.fee_open_cost = trade.fee_open * float(trade.max_stake_amount)
            trade.recalc_open_trade_value()
            if trade.stop_loss_pct is not None and trade.open_rate is not None:
                trade.adjust_stop_loss(trade.open_rate, trade.stop_loss_pct)
        elif is_closing and total_stake > 0:
            # Close profit abs / maximum owned
            # Fees are considered as they are part of close_profit_abs
            trade.close_profit = (close_profit_abs / total_stake) * trade.leverage
            trade.close_profit_abs = close_profit_abs
    # async def process_trades(self):
    #     if not self.strategy.loop_enable :
    #          return 
    #     trades: list[Trade] = Trade.get_open_trades()

    #     self.active_pair_whitelist = self._refresh_active_whitelist(trades)

    #     # Refreshing candles
    #     pair_list =self.dataprovider.merge_pairs_helperpairs(self.pairlists.create_pair_list(self.active_pair_whitelist),
    #         self.strategy.gather_informative_pairs())
    #     self.dataprovider.refresh_latest_trades(
    #         pair_list
    #     )
     
    #     with self._exit_lock:
    #         # Check for exchange cancellations, timeouts and user requested replace
    #         self.manage_open_orders()

    #     # Protect from collisions with force_exit.
    #     # Without this, freqtrade may try to recreate stoploss_on_exchange orders
    #     # while exiting is in process, since telegram messages arrive in an different thread.
    #     with self._exit_lock:
    #         trades = Trade.get_open_trades()
    #         # First process current opened trades (positions)
    #         self.exit_positions(trades)
    #         Trade.commit()

    #     # Check if we need to adjust our current positions before attempting to enter new trades.
    #     if self.strategy.position_adjustment_enable:
    #         with self._exit_lock:
    #                 self.process_open_trade_positions()

    #     # Then looking for entry opportunities
    #     if self.state == State.RUNNING and self.get_free_open_trades():
    #         whitelist = self._get_nolock_whitelist()
    #         if not whitelist:
    #             self.log_once("Active pair whitelist is empty.", logger.info)
    #         else:
    #             trades_created = 0
    #     # Create entity and execute trade for each pair from whitelist
    #             for pair in whitelist:
    #                 try:
    #                     with self._exit_lock:
    #                         if self.strategy.loop_enable :
    #                             trades_created += self._create_trade_loop(pair)
                           
    #                 except DependencyException as exception:
    #                     logger.warning("Unable to create trade for %s: %s", pair, exception)

    #                 if not trades_created:
    #                     logger.debug("Found no enter signals for whitelisted currencies. Trying again...")
               
            
    #     self._schedule.run_pending()
    #     Trade.commit()
    #     self.rpc.process_msg_queue(self.dataprovider._msg_queue)
    #     self.last_process = datetime.now(timezone.utc)

    def _get_bidirectional_pairs(self):

        trade_pairs:dict[str,str] = {}
        
        for trade in Trade.get_open_trades():
            trade = cast(Trade, trade)
            pair = trade.pair
            direcation: str = trade_pairs.get(pair, "")
            if trade.trade_direction not in direcation:
                trade_pairs[pair] = direcation + trade.trade_direction
        return trade_pairs

    # def _check_pair_direction_match(self, pair: str, signal: str | None, can_hedge_mode: bool):
    #     """Check if trading pair direction matches the given signal under current hedging mode.

    #     Args:
    #         pair: Trading pair identifier (e.g. 'BTC/USD')
    #         signal: Trading direction signal from strategy, None indicates no signal
    #         can_hedge_mode: Flag indicating if hedge trading mode is enabled

    #     Returns:
    #         bool: True if direction matches requirements, False otherwise

    #     Note:
    #         Decision logic depends on bidirectional pairs configuration and hedging mode status
    #     """
    #     pairs_directions = self._get_bidirectional_pairs()
    #     direction = pairs_directions.get(pair, "")
    #     if pair not in pairs_directions:
    #         # open
    #         return False
    #     elif (not can_hedge_mode) or (signal is None) or (len(signal) == len(direction)) or (signal in direction):
    #         # not open
    #         return True
    #     else:
    #         return False 
    def _get_nolock_whitelist(self,can_hedge_mode: bool=False) -> dict[str,str]|None:
        """
        获取非锁定状态下的白名单，若存在全局锁定则返回空列表或 None。
        """
        # 创建白名单的深拷贝
        whitelist = deepcopy(self.active_pair_whitelist)
        
        # 如果白名单为空，记录日志并返回
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
            return None
        
        # 检查全局锁定状态
        if PairLocks.is_global_lock(side="*"):
            # 全局锁定存在时，记录日志并返回空列表
            lock = PairLocks.get_pair_longest_lock("*")
            if lock:
                self.log_once(
                    f"Global pairlock active until "
                    f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)}. "
                    f"Not creating new trades, reason: {lock.reason}.",
                    logger.info,
                )
            else:
                self.log_once("Global pairlock active. Not creating new trades.", logger.info)
            
            # 显式返回空列表，表示因锁定无法创建新交易
            return []
    
        # 所有检查通过，返回白名单
        tradespairs = self._get_bidirectional_pairs()
        for pair in whitelist:
            if pair in tradespairs:
                match len( tradespairs[pair]) :
                    case 4 | 5  if not can_hedge_mode:
                        del tradespairs[pair]
                    case 9:
                        del tradespairs[pair]
                    case _:
                        del tradespairs[pair]
            else:
                tradespairs[pair]=""
        return tradespairs
   
    #
    # enter positions / open trades logic and methods
    #
    def _create_trade_bytickle(self, pair:str,df:DataFrame,direction=""):
        # current_time = self.dataprovider.orderbook(pair=pair,maximum=1)["timestamp"]
        result = self.strategy._loop_entry(pair=pair,timestamp=dt_now(),df=df)
        if result is None:
            return False
        signal,stake_amount,price,entry_tag= result
        # not_opening = not self._check_pair_direction_match(
        #     pair=pair, signal=signal, can_hedge_mode=self.strategy.can_hedge_mode
        # )
        if not signal or signal == direction or signal in direction:
            self.logger.info(f" trade_loop,not opening {pair} because of direction mismatch")
            return False
        stake_amount = stake_amount if stake_amount else self.wallets.get_trade_stake_amount(
                pair, self.config["max_open_trades"], self.edge
            )
        return self.execute_entry(pair=pair, stake_amount=stake_amount, price=price,is_short=(signal==SignalDirection.SHORT),enter_tag=entry_tag)
    def enter_positions(self) -> int:
        whitelist = self._get_nolock_whitelist(can_hedge_mode=self.strategy.can_hedge_mode)
       
       
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
        else:
            trades_created = 0
            for pair,direction in whitelist.items():
                try:
                    analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair=pair, timeframe=self.strategy.timeframe)
                    with self._exit_lock:
                            if self.strategy.loop_enable :
                                trades_created += self._create_trade_bytickle(pair,analyzed_df,direction)
                            else:
                                trades_created += self.create_trade(pair,analyzed_df,direction)
                except DependencyException as exception:
                    logger.warning("Unable to create trade for %s: %s", pair, exception)
                if not trades_created:
                    logger.debug("Found no enter signals for whitelisted currencies. Trying again...")
        return trades_created

    def create_trade(self, pair: str,df:DataFrame|None =None,direction="") -> bool:
        """
        Check the implemented trading strategy for entry signals.

        If the pair triggers the enter signal a new trade record gets created
        and the entry-order opening the trade gets issued towards the exchange.

        :return: True if a trade has been created.
        """
       
        if df:
            analyzed_df = df
        else:
            analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair, self.strategy.timeframe)
        nowtime = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None

        

        # running get_signal on historical data fetched
        #价格或交易都可以直接返回none
        signal, enter_tag = self.strategy.get_entry_signal(
            pair, self.strategy.timeframe, analyzed_df
        )
       
        if not signal or signal == direction or signal in direction:
            self.logger.info(f" trade_loop,not opening {pair} because of direction mismatch")
            return False
        if self.strategy.is_pair_locked(pair, candle_date=nowtime, side=signal):
            lock = PairLocks.get_pair_longest_lock(pair, nowtime, signal)
            if lock:
                self.log_once(
                    f"Pair {pair} {lock.side} is locked until "
                    f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)} "
                    f"due to {lock.reason}.",
                    logger.info,
                )
            else:
                self.log_once(f"Pair {pair} is currently locked.", logger.info)
            return False
        stake_amount = self.wallets.get_trade_stake_amount(
            pair, self.config["max_open_trades"], self.edge
        )
        bid_check_dom = self.config.get("entry_pricing", {}).get("check_depth_of_market", {})
        if (bid_check_dom.get("enabled", False)) and (
            bid_check_dom.get("bids_to_ask_delta", 0) > 0
        ):
            if self._check_depth_of_market(pair, bid_check_dom, side=signal):
                return self.execute_entry(
                    pair,
                    stake_amount,
                    enter_tag=enter_tag,
                    is_short=(signal == SignalDirection.SHORT),
                )
            else:
                return False
        
        
        return self.execute_entry(
            pair=pair, stake_amount=stake_amount ,enter_tag=enter_tag, is_short=(signal == SignalDirection.SHORT)
        )
      

