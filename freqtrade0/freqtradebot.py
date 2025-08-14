"""
Freqtrade is the main module of this bot. It contains the class Freqtrade()
"""

import asyncio
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from threading import Lock
from typing import cast

import jsonschema
from pandas import DataFrame
from schedule import Scheduler

import freqtrade.freqtradebot
from freqtrade.configuration import validate_config_consistency
from freqtrade.constants import Config, ExchangeConfig
from freqtrade.enums import (
    ExitCheckTuple,
    ExitType,
    MarginMode,
    SignalDirection,
    State,
    TradingMode,
)
from freqtrade.exceptions import DependencyException
from freqtrade.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange import Exchange
from freqtrade.mixins import LoggingMixin
from freqtrade.persistence import PairLocks, Trade, init_db
from freqtrade.persistence.models import PairLock
from freqtrade.persistence.trade_model import ProfitStruct
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.rpc import RPCManager
from freqtrade.rpc.external_message_consumer import ExternalMessageConsumer
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, MeasureTime, PeriodicCache, dt_now
from freqtrade.wallets import Wallets
from freqtrade0.enums import TradeDirection


logger = freqtrade.freqtradebot.logger
class FreqtradeBot(freqtrade.freqtradebot.FreqtradeBot):
    """
    Freqtrade is the main class of the bot.
    This is from here the bot start its logic.
    """

 
    def log_once(self, message: str, logmethod: Callable, force_show: bool = False) -> None:
        """
        Logs message - not more often than "refresh_period" to avoid log spamming
        Logs the log-message as debug as well to simplify debugging.
        :param message: String containing the message to be sent to the function.
        :param logmethod: Function that'll be called. Most likely `logger.info`.
        :param force_show: If True, sends the message regardless of show_output value.
        :return: None.
        """
        now_time = dt_now()
        internal = timedelta(seconds=self.refresh_period)
        delkeys = []
        for k  ,v in self.log_cache.items():
            if now_time - v > internal:
                delkeys.append(k)
        for k in delkeys:
            del self.log_cache[k]
        if message not in self.log_cache:
            logmethod(message)
            self.log_cache[message] = now_time

    def _getpairlist(self,informative_pairlist):
        trades: list[Trade] = Trade.get_open_trades()
        self.active_pair_whitelist = self._refresh_active_whitelist(trades)
        pairlist = self.pairlists.create_pair_list(self.active_pair_whitelist)
        res_pair_list = pairlist + informative_pairlist if informative_pairlist else pairlist
        return res_pair_list

    def _get_ohlcv_set(self):
        informative_pairlist =self.strategy.gather_informative_pairs()
        _pairs = self._getpairlist(informative_pairlist)
        return set(_pairs)



    def _get_tradesset(self):
        informative_pairlist =self.strategy.gather_informative_trade_pairs()
        trade_pairs = self._getpairlist(informative_pairlist)
        return set(trade_pairs)

  







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
 


    def create_trade(self, pair: str,*,direction:TradeDirection=TradeDirection.NONE,df:DataFrame|None =None) -> bool:
        """
        Check the implemented trading strategy for entry signals.

        If the pair triggers the enter signal a new trade record gets created
        and the entry-order opening the trade gets issued towards the exchange.

        :return: True if a trade has been created.
        """

        if df is not None and df.empty == False:
            analyzed_df = df
        else:
            analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair, self.strategy.timeframe)
        # nowtime = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None



        # running get_signal on historical data fetched
        #价格或交易都可以直接返回none
        signal, enter_tag = self.strategy.get_entry_signal(
            pair, self.strategy.timeframe, analyzed_df
        )
        if  signal is None  or TradeDirection.convert(signal) & direction > TradeDirection.NONE :
            return False
        stake_amount = self.wallets.get_trade_stake_amount(
            pair, self.config["max_open_trades"]
        )
        bid_check_dom = self.config.get("entry_pricing", {}).get("check_depth_of_market", {})
        if (bid_check_dom.get("enabled", False)) and (
            bid_check_dom.get("bids_to_ask_delta", 0) > 0
        ):
            if self._check_depth_of_market(pair, bid_check_dom, side=signal):
                return self.execute_entry(
                    pair,
                    stake_amount=stake_amount,
                    enter_tag=enter_tag,
                    is_short=(signal == SignalDirection.SHORT),
                )
            else:
                return False


        return self.execute_entry(
            pair=pair, stake_amount=stake_amount ,enter_tag=enter_tag, is_short=(signal == SignalDirection.SHORT)
        )

    def enter_positions(self) -> int:
        whitelist = self._get_nolock_whitelist(can_hedge_mode=self.strategy.can_hedge_mode)
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
                            trades_created_ohlc += self.create_trade(pair,direction= direction,df=analyzed_df)
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
