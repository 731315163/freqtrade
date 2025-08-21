"""
IStrategy interface
This module defines the interface to apply for strategies
"""

from datetime import UTC, datetime, timedelta
from re import match
from typing import Literal

import pandas as pd
from numpy import long
from pandas import DataFrame

from freqtrade import strategy
from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.enums import (
    CandleType,
)
from freqtrade.enums.signaltype import SignalDirection, SignalTagType, SignalType
from freqtrade.enums.tradingmode import TradingMode
from freqtrade.exceptions import StrategyError
from freqtrade.exchange import timeframe_to_minutes
from freqtrade.exchange.exchange_utils_timeframe import timeframe_to_seconds
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.misc import remove_entry_exit_signals
from freqtrade.persistence import Trade
from freqtrade.strategy.informative_decorator import (
    _format_pair_name,
)
from freqtrade.strategy.interface import logger, remove_entry_exit_signals
from freqtrade.strategy.strategy_validation import StrategyResultValidator
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import dt_now
from freqtrade.util.datetime_helpers import dt_now
from freqtrade0.enums import TradeDirection


class IStrategy(strategy.IStrategy):
    """
    Interface for freqtrade strategies
    Defines the mandatory structure must follow any custom strategies

    Attributes you can use:
        minimal_roi -> Dict: Minimal ROI designed for the strategy
        stoploss -> float: optimal stoploss designed for the strategy
        timeframe -> str: value of the timeframe to use with the strategy
    """


    can_short: bool = True
    can_hedge_mode: bool = True

    def __init__(self, config: Config) -> None:
        self.bot :FreqtradeBot= None
        super().__init__(config)

    def informative_trade_pairs(self) -> ListPairsWithTimeframes:
        """
        Define additional, informative pair/interval combinations to be cached from the exchange.
        These pair/interval combinations are non-tradable, unless they are part
        of the whitelist as well.
        For more information, please consult the documentation
        :return: List of tuples in the format (pair, interval)
            Sample: return [("ETH/USDT", "5m"),
                            ("BTC/USDT", "15m"),
                            ]
        """
        return []
    def gather_informative_trade_pairs(self) -> ListPairsWithTimeframes:
        """
        Internal method which gathers all informative pairs (user or automatically defined).
        """
        informative_pairs = self.informative_trade_pairs()
        # Compatibility code for 2 tuple informative pairs
        informative_pairs = [
            (
                p[0],
                p[1],
                (
                    CandleType.from_string(p[2])
                    if len(p) > 2 and p[2] != ""
                    else self.config.get("candle_type_def", CandleType.SPOT)
                ),
            )
            for p in informative_pairs
        ]
        for inf_data, _ in self._ft_informative:
            # Get default candle type if not provided explicitly.
            candle_type = (
                inf_data.candle_type
                if inf_data.candle_type
                else self.config.get("candle_type_def", CandleType.SPOT)
            )
            if inf_data.asset:
                if any(s in inf_data.asset for s in ("{BASE}", "{base}")):
                    for pair in self.dp.current_whitelist():
                        pair_tf = (
                            _format_pair_name(self.config, inf_data.asset, self.dp.market(pair)),
                            inf_data.timeframe,
                            candle_type,
                        )
                        informative_pairs.append(pair_tf)

                else:
                    pair_tf = (
                        _format_pair_name(self.config, inf_data.asset),
                        inf_data.timeframe,
                        candle_type,
                    )
                    informative_pairs.append(pair_tf)
            else:
                for pair in self.dp.current_whitelist():
                    informative_pairs.append((pair, inf_data.timeframe, candle_type))
        informative_pairs.extend(self.__informative_pairs_freqai())
        return list(set(informative_pairs))



    # def loop_entry(self,pair:str,timestamp:datetime) ->None| tuple[Literal["long","short"],float|None]|tuple[Literal["long","short"],float|None,float|None|str]|tuple[Literal["long","short"],float|None,float|None,str]:

    #     '''
    #     return tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
    #     return a tuple of (side,amount,price,signal_name)|None for the entry signal
    #     *side: "long" or "short",If the side is none, no action will be performed.
    #     stack: float | None
    #     price: float | None
    #     signal_name: str
    #     '''
        
    #     pass



    # def _loop_entry(
    #     self,pair:str,timestamp:datetime,
    #     df :DataFrame|None,
    #     **kwargs
    # ) -> tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
    #     """
    #     wrapper around adjust_trade_position to handle the return value
    #     """
    #     # lastes,latest_time= self.get_latest_candle(pair,self.timeframe,df)
    #     resp = strategy_safe_wrapper(
    #         self.loop_entry, default_retval=(None, ""), supress_error=True
    #     )(
    #        pair = pair,timestamp = timestamp,
    #         **kwargs
    #     )

    #     result = None
    #     match resp:
    #         case (side, stake_amount, price, order_tag):
    #             result=( side, stake_amount, price, order_tag)
    #         case (side, stake_amount, price_or_tag):
    #             if isinstance(price_or_tag, str):
    #                 result=( side, stake_amount, None, price_or_tag)
    #             else:
    #                 result=( side, stake_amount, price_or_tag, "")
    #         case (side, stake_amount):
    #             result=( side, stake_amount, None, "")
    #         case _:
    #             return None

    #     return result

    def populate_all(self, dataframes:dict[str,tuple[ DataFrame,bool]], **kwargs) ->dict[str,tuple[ DataFrame,bool]]:
        return dataframes

    def _analyze_all_signals(self, dataframes:dict[str,tuple[ DataFrame,bool]],candle_type, **kwargs) ->dict[str, tuple[ DataFrame,bool]]:
        result_dataframe= self.populate_all(dataframes, **kwargs)
        for pair, (dataframe,new_candle) in result_dataframe.items():
            self.dp._set_cached_df(pair, self.timeframe, dataframe, candle_type=candle_type)
            self.dp._emit_df((pair, self.timeframe, candle_type), dataframe, new_candle)
        return result_dataframe


    def _analyze_ticker_signals(self, dataframe: DataFrame, metadata: dict) :
        """
        Parses the given candle (OHLCV) data and returns a populated DataFrame
        add several TA indicators and buy signal to it
        WARNING: Used internally only, may skip analysis if `process_only_new_candles` is set.
        :param dataframe: Dataframe containing data from exchange
        :param metadata: Metadata dictionary with additional data (e.g. 'pair')
        :return: DataFrame of candle (OHLCV) data with indicator data and signals added
        """
        pair = str(metadata.get("pair"))
        _last_seen = self._last_candle_seen_per_pair.get(pair, datetime.min.replace(tzinfo=UTC))
        last_date = dataframe.iloc[-1]["date"]
        new_candle = False
        if last_date > _last_seen:
            new_candle = True
        elif last_date < _last_seen:
            raise ValueError(
                f"Last candle seen for {pair} is {_last_seen}, "
                f"but last candle in dataframe is {last_date}"
            )


        # Test if seen this pair and last candle before.
        # always run if process_only_new_candles is set to false
        if not self.process_only_new_candles or new_candle:
            # Defs that only make change on new candle data.
            dataframe = self.analyze_ticker(dataframe, metadata)
            self._last_candle_seen_per_pair[pair] = dataframe.iloc[-1]["date"]

        else:
            logger.debug("Skipping TA Analysis for already analyzed candle")
            dataframe = remove_entry_exit_signals(dataframe)

        return dataframe,new_candle
    def process_pair_data(self, pair: str,dataframe:DataFrame) :
        """
        Stores the dataframe into the dataprovider.
        The analyzed dataframe is then accessible via `dp.get_analyzed_dataframe()`.
        :param pair: Pair to analyze.
        """

        if not isinstance(dataframe, DataFrame) or dataframe.empty:
            # logger.warning("Empty candle (OHLCV) data for pair %s", pair)
            return ()

        try:
            validator = StrategyResultValidator(  dataframe, warn_only=not self.disable_dataframe_checks)

            dataframe,new_candle = strategy_safe_wrapper(self._analyze_ticker_signals, message="")(dataframe, {"pair": pair})

            validator.assert_df(dataframe)
        except StrategyError as error:
            logger.warning(f"Unable to analyze candle (OHLCV) data for pair {pair}: {error}")
            return ()

        if dataframe.empty:

            # logger.warning("Empty dataframe for pair %s", pair)
            return ()
        return dataframe,new_candle


    def analyze(self, pairs: list[str]) -> None:
        """
        Analyze all pairs using analyze_pair().
        :param pairs: List of pairs to analyze
        """
        candle_type=self.config.get("candle_type_def", CandleType.SPOT)
        pair_data = {}
        for pair in pairs:
            dataframe = self.dp.ohlcv(
            pair, self.timeframe, candle_type=candle_type
        )
            result =  self.process_pair_data(pair, dataframe)
            if len(result) > 0:
                dataframe,new_candle =result
                pair_data[pair]=( dataframe,new_candle)
        pair_data = self._analyze_all_signals(dataframes=pair_data,candle_type=candle_type)
    def get_entry_signal(
        self,
        pair: str,
        timeframe: str,
        dataframe: DataFrame,
    ) -> tuple[TradeDirection | None, str | tuple|None]:
        """
        双向持仓大量修改
        Calculates current entry signal based based on the dataframe signals
        columns of the dataframe.
        Used by Bot to get the signal to enter trades.
        :param pair: pair in format ANT/BTC
        :param timeframe: timeframe to use
        :param dataframe: Analyzed dataframe to get signal from.
        :return: (SignalDirection, entry_tag)
        """
        latest, latest_date = self.get_latest_candle(pair, timeframe, dataframe)
        if latest is None or latest_date is None:
            return None, None

        enter_long = latest.get(SignalType.ENTER_LONG.value, 0) == 1
        exit_long = latest.get(SignalType.EXIT_LONG.value, 0) == 1
        enter_short = latest.get(SignalType.ENTER_SHORT.value, 0) == 1
        exit_short = latest.get(SignalType.EXIT_SHORT.value, 0) == 1

        enter_signal: TradeDirection  = TradeDirection.NONE
        long_enter_tag: str | None = None
        short_enter_tag: str | None = None
        if enter_long == 1 and not any([exit_long]):
            enter_signal = enter_signal| TradeDirection.LONG
            long_enter_tag = latest.get(SignalTagType.ENTER_TAG.value, None)
        if (
            self.config.get("trading_mode", TradingMode.SPOT) != TradingMode.SPOT
            and self.can_short
            and enter_short == 1
            and not any([exit_short])
        ):
            enter_signal =enter_signal | TradeDirection.SHORT
            short_enter_tag = latest.get(SignalTagType.ENTER_TAG.value, None)

     

        timeframe_seconds = timeframe_to_seconds(timeframe)

        if self.ignore_expired_candle(
            latest_date=latest_date,
            current_time=dt_now(),
            timeframe_seconds=timeframe_seconds,
            enter=bool(enter_signal),
        ):
            return None, None
        match enter_signal:
            case TradeDirection.BOTH:
                enter_tag = (long_enter_tag, short_enter_tag)
            case TradeDirection.LONG:
                enter_tag = long_enter_tag
            case TradeDirection.SHORT:
                enter_tag = short_enter_tag
            case _:
                enter_tag = None
            
        return enter_signal, enter_tag

    def _adjust_trade_position_internal(
        self,
        trade: Trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        min_stake: float | None,
        max_stake: float,
        current_entry_rate: float,
        current_exit_rate: float,
        current_entry_profit: float,
        current_exit_profit: float,
        **kwargs,
    ) -> list[tuple[float | None, float,str]]:
        """
        wrapper around adjust_trade_position to handle the return value
        可以返回多个订单，包括(stake_amount,price,tag) ,stake_amount,或者list 多个订单
        profit_struc in kwargs 参数是所有成交顶订单的总利润，不只是剩余订单的利润，原版位剩余订单利润，请注意
        """
        _ordersORresp = strategy_safe_wrapper(
            self.adjust_trade_position, default_retval=(None,current_rate, ""), supress_error=True
        )(
            trade=trade,
            current_time=current_time,
            current_rate=current_rate,
            current_profit=current_profit,
            min_stake=min_stake,
            max_stake=max_stake,
            current_entry_rate=current_entry_rate,
            current_exit_rate=current_exit_rate,
            current_entry_profit=current_entry_profit,
            current_exit_profit=current_exit_profit,
            **kwargs,
        )
        _orders =_ordersORresp if  isinstance(_ordersORresp, list) else [_ordersORresp]

        def def_price(stake_amount:float|None):
            if stake_amount is None:
                return 0.0
            elif stake_amount < 0:
                return current_exit_rate
            else:
                return current_entry_rate
        result =[]
        for resp in _orders:
            resp_tuple = resp if isinstance(resp, tuple) else (resp,)
            match resp_tuple:
                case (stake_amount, price, order_tag):
                    result.append( (stake_amount, price, order_tag))
                case (stake_amount, price_or_tag):
                    if isinstance(price_or_tag, str):
                        result.append((stake_amount, def_price(stake_amount) , price_or_tag))
                    else:
                        result.append((stake_amount, price_or_tag, ""))
                case (stake_amount, ):
                    result.append(( stake_amount, def_price( stake_amount), ""))
                case _:
                    continue
        return result
    # def get_latest_candle(
    #     self,
    #     pair: str,
    #     timeframe: str,
    #     dataframe: DataFrame,
    # ) -> tuple[DataFrame | None, datetime | None]:
    #     """
    #     保留此函数，修复其不在istrtegy中就出现outdated history 错误
    #     Calculates current signal based based on the entry order or exit order
    #     columns of the dataframe.
    #     Used by Bot to get the signal to enter, or exit
    #     :param pair: pair in format ANT/BTC
    #     :param timeframe: timeframe to use
    #     :param dataframe: Analyzed dataframe to get signal from.
    #     :return: (None, None) or (Dataframe, latest_date) - corresponding to the last candle
    #     """
    #     if not isinstance(dataframe, DataFrame) or dataframe.empty:
    #         logger.error(f"Empty candle (OHLCV) data for pair {pair}")
    #         return None, None

     
    #     latest_date_pd = dataframe["date"].max()
        
    #     latest = dataframe.loc[dataframe["date"] == latest_date_pd].iloc[-1]
    #     # except Exception as e:
    #     #     logger.error(f"Unable to get latest candle (OHLCV) data for pair {pair} - {e}")
    #     #     return None, None
    #     # Explicitly convert to datetime object to ensure the below comparison does not fail
    #     latest_date: datetime = latest_date_pd.to_pydatetime()
    #     # Check if dataframe is out of date
    #     timeframe_minutes = timeframe_to_minutes(timeframe)
    #     offset = self.config.get("exchange", {}).get("outdated_offset", 2)
    #     if latest_date < (dt_now() - timedelta(minutes=timeframe_minutes * 2 + offset)):
    #         logger.critical(
    #             f"Outdated history for {pair} pair. Last tick is {int((dt_now() - latest_date).total_seconds() // 60)} minutes old"
    #                         )
    #         return None, None
    #     return latest, latest_date
