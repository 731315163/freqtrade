# pragma pylint: disable=missing-docstring, C0103
# pragma pylint: disable=protected-access, too-many-lines, invalid-name, too-many-arguments

import logging
import time
from copy import deepcopy
from datetime import timedelta
from unittest.mock import ANY, MagicMock, PropertyMock, patch

import pytest
from pandas import DataFrame
from sqlalchemy import select

from freqtrade.constants import CANCEL_REASON, UNLIMITED_STAKE_AMOUNT
from freqtrade.enums import (
    CandleType,
    ExitCheckTuple,
    ExitType,
    RPCMessageType,
    RunMode,
    SignalDirection,
    State,
)
from freqtrade.exceptions import (
    DependencyException,
    ExchangeError,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    PricingError,
    TemporaryError,
)
from freqtrade.persistence import Order, PairLocks, Trade
from freqtrade.plugins.protections.iprotection import ProtectionReturn
from freqtrade.util.datetime_helpers import dt_now, dt_utc
from freqtrade.worker import Worker
from freqtrade0.enums import TradeDirection
from freqtrade0.freqtradebot import FreqtradeBot
from tests0.conftest import (
    EXMS,
    create_mock_trades,
    create_mock_trades_usdt,
    get_patched_freqtradebot,
    get_patched_worker,
    log_has,
    log_has_re,
    patch_exchange,
    patch_get_signal,
    patch_wallet,
    patch_whitelist,
)
from tests0.conftest_trades import (
    MOCK_TRADE_COUNT,
    entry_side,
    exit_side,
    mock_order_2,
    mock_order_2_sell,
    mock_order_3,
    mock_order_3_sell,
    mock_order_4,
    mock_order_5_stoploss,
    mock_order_6_sell,
)
from tests0.conftest_trades_usdt import mock_trade_usdt_4


def patch_RPCManager(mocker) -> MagicMock:
    """
    This function mock RPC manager to avoid repeating this code in almost every tests
    :param mocker: mocker to patch RPCManager class
    :return: RPCManager.send_msg MagicMock to track if this method is called
    """
    mocker.patch("freqtrade.rpc.telegram.Telegram", MagicMock())
    rpc_mock = mocker.patch("freqtrade.freqtradebot.RPCManager.send_msg", MagicMock())
    return rpc_mock

# test0




def test_hedge_mode_trades_open(
    default_conf_usdt, ticker_usdt, fee, mocker, limit_buy_order_usdt_open, caplog
) -> None:
    
    patch_RPCManager(mocker)
    patch_exchange(mocker)
    default_conf_usdt["max_open_trades"] = 4
    mocker.patch.multiple(
        EXMS,
        fetch_ticker=ticker_usdt,
        create_order=MagicMock(return_value=limit_buy_order_usdt_open),
        get_fee=fee,
    )
    freqtrade = FreqtradeBot(default_conf_usdt)
    freqtrade.exchange.refresh_latest_ohlcv = lambda p: None
    freqtrade.strategy.get_entry_signal = lambda pair, timeframe, analyzed_df :("long","L")
   

    # Create 2 existing trades
    assert freqtrade.create_trade("ETH/USDT")
    assert freqtrade.create_trade("NEO/BTC")
   

    assert len(Trade.get_open_trades()) == 2
    # Change order_id for new orders
    limit_buy_order_usdt_open["id"] = "123444"
    freqtrade.strategy.get_entry_signal = lambda pair, timeframe, analyzed_df :("short","S")
    # Create 2 new trades using create_trades
    assert freqtrade.create_trade("ETH/USDT")
    assert freqtrade.create_trade("NEO/BTC")

    trades = Trade.get_open_trades()
    assert len(trades) == 4

def test_hedge_mode_entryposition(
    default_conf_usdt, ticker_usdt, fee, mocker, limit_buy_order_usdt_open, caplog
) -> None:
    
    patch_RPCManager(mocker)
    patch_exchange(mocker)
    default_conf_usdt["max_open_trades"] = 4
    mocker.patch.multiple(
        EXMS,
        fetch_ticker=ticker_usdt,
        create_order=MagicMock(return_value=limit_buy_order_usdt_open),
        get_fee=fee,
    )
    freqtrade = FreqtradeBot(default_conf_usdt)
    freqtrade.exchange.refresh_latest_ohlcv = lambda p: None
    freqtrade.strategy.get_entry_signal = lambda pair, timeframe, analyzed_df :("long","L")
    freqtrade._get_nolock_whitelist=lambda can_hedge_mode:{"ETH/USDT":TradeDirection.NONE,"NEO/BTC":TradeDirection.NONE}

    # Create 2 existing trades
    assert freqtrade.create_trade("ETH/USDT")
    assert freqtrade.create_trade("NEO/BTC")
   

    assert len(Trade.get_open_trades()) == 2
    # Change order_id for new orders
    limit_buy_order_usdt_open["id"] = "123444"
    freqtrade.strategy.get_entry_signal = lambda pair, timeframe, analyzed_df :("short","S")
    freqtrade._get_nolock_whitelist=lambda can_hedge_mode:{"ETH/USDT":TradeDirection.LONG,"NEO/BTC":TradeDirection.Long}
    # Create 2 new trades using create_trades
    assert freqtrade.create_trade("ETH/USDT")
    assert freqtrade.create_trade("NEO/BTC")

    trades = Trade.get_open_trades()
    assert len(trades) == 4


def test_check_and_call_adjust_trade_position_hedge(mocker, default_conf_usdt, fee, caplog) -> None:
    default_conf_usdt.update(
        {
            "position_adjustment_enable": True,

        }
    )
    freqtrade = get_patched_freqtradebot(mocker, default_conf_usdt)
    buy_rate_mock = MagicMock(return_value=10)
    mocker.patch.multiple(
        EXMS,
        get_rate=buy_rate_mock,
        fetch_ticker=MagicMock(return_value={"bid": 10, "ask": 12, "last": 11}),
        get_min_pair_stake_amount=MagicMock(return_value=1),
        get_fee=fee,
    )
    create_mock_trades(fee)
    caplog.set_level(logging.DEBUG)
    freqtrade.strategy.adjust_trade_position = MagicMock(return_value=[(10, "aaaa"),(10,"bbbb")])
    freqtrade.process_open_trade_positions()
    assert freqtrade.strategy.adjust_trade_position.call_count == 4

   
    freqtrade.strategy.adjust_trade_position = MagicMock(return_value=[(-0.0005, "partial_exit_c")])
    freqtrade.process_open_trade_positions()
    assert log_has_re(r"LIMIT_SELL has been fulfilled.*", caplog)
    assert freqtrade.strategy.adjust_trade_position.call_count == 4
    trade = Trade.get_trades(trade_filter=[Trade.id == 5]).first()
    assert trade.orders[-1].ft_order_tag == "partial_exit_c"
    assert trade.is_open


def test_check_and_call_adjust_trade_position_orders_hedge(mocker, default_conf_usdt, fee, caplog) -> None:
    default_conf_usdt.update(
        {
            "position_adjustment_enable": True,

        }
    )
    freqtrade = get_patched_freqtradebot(mocker, default_conf_usdt)
    buy_rate_mock = MagicMock(return_value=10)
    mocker.patch.multiple(
        EXMS,
        get_rate=buy_rate_mock,
        fetch_ticker=MagicMock(return_value={"bid": 10, "ask": 12, "last": 11}),
        get_min_pair_stake_amount=MagicMock(return_value=1),
        get_fee=fee,
    )
    create_mock_trades(fee)
    caplog.set_level(logging.DEBUG)
    freqtrade.strategy.adjust_trade_position = MagicMock(return_value=[(10, "aaaa"),(10,"bbbb")])
    freqtrade.process_open_trade_positions()
    trades = Trade.get_open_trades()
    for trade in trades:
        assert trade.orders[-1].ft_order_tag == "bbbb"

   
    freqtrade.strategy.adjust_trade_position = MagicMock(return_value=[(10,10, "1"),(10,20, "2"),(10,10, "3"),(-10,20, "4")])
    freqtrade.process_open_trade_positions()
    assert log_has_re(r"LIMIT_SELL has been fulfilled.*", caplog)
    assert freqtrade.strategy.adjust_trade_position.call_count == 4
    trade = Trade.get_trades(trade_filter=[Trade.id == 5]).first()
    assert trade.orders[-1].ft_order_tag == "4"
    assert trade.is_open








# Unit tests