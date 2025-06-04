"""
IStrategy interface
This module defines the interface to apply for strategies
"""

import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Literal

from pandas import DataFrame


from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.enums import (
    CandleType,
)
from freqtrade.exceptions import OperationalException
from freqtrade.exchange import timeframe_to_minutes
from freqtrade.persistence.trade_model import Trade
from freqtrade.strategy.informative_decorator import (
    InformativeData,
    PopulateIndicators,
    _format_pair_name,
)
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
import freqtrade.strategy
from freqtrade.util.datetime_helpers import dt_now

logger = logging.getLogger(__name__)


class IStrategy(freqtrade.strategy.IStrategy):
    """
    Interface for freqtrade strategies
    Defines the mandatory structure must follow any custom strategies

    Attributes you can use:
        minimal_roi -> Dict: Minimal ROI designed for the strategy
        stoploss -> float: optimal stoploss designed for the strategy
        timeframe -> str: value of the timeframe to use with the strategy
    """

   

    can_hedge_mode: bool = False
    loop_enable: bool = True
    def __init__(self, config: Config) -> None:
        self.config = config
        # Dict to determine if analysis is necessary
        self._last_candle_seen_per_pair: dict[str, datetime] = {}
        super().__init__(config)

        # Gather informative pairs from @informative-decorated methods.
        self._ft_informative: list[tuple[InformativeData, PopulateIndicators]] = []
        for attr_name in dir(self.__class__):
            cls_method = getattr(self.__class__, attr_name)
            if not callable(cls_method):
                continue
            informative_data_list = getattr(cls_method, "_ft_informative", None)
            if not isinstance(informative_data_list, list):
                # Type check is required because mocker would return a mock object that evaluates to
                # True, confusing this code.
                continue
            strategy_timeframe_minutes = timeframe_to_minutes(self.timeframe)
            for informative_data in informative_data_list:
                if timeframe_to_minutes(informative_data.timeframe) < strategy_timeframe_minutes:
                    raise OperationalException(
                        "Informative timeframe must be equal or higher than strategy timeframe!"
                    )
                if not informative_data.candle_type:
                    informative_data.candle_type = config["candle_type_def"]
                self._ft_informative.append((informative_data, cls_method))
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
    
    
    
    def loop_entry(self,pair:str,timestamp:datetime) ->None| tuple[Literal["long","short"],float|None]|tuple[Literal["long","short"],float|None,float|None|str]|tuple[Literal["long","short"],float|None,float|None,str]:
        
        '''
        return tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
        return a tuple of (side,amount,price,signal_name)|None for the entry signal
        *side: "long" or "short",If the side is none, no action will be performed.
        stack: float | None
        price: float | None
        signal_name: str
        '''
        pass
    def cache_dataframe(self, dataframe: DataFrame, pair:str,side:Literal["long","short"],tag="") -> DataFrame|None:
        """
        Parses the given candle (OHLCV) data and returns a populated DataFrame
        add several TA indicators and buy signal to it
        WARNING: Used internally only, may skip analysis if `process_only_new_candles` is set.
        :param dataframe: Dataframe containing data from exchange
        :param metadata: Metadata dictionary with additional data (e.g. 'pair')
        :return: DataFrame of candle (OHLCV) data with indicator data and signals added
        """
        if  "enter_long" not in dataframe.columns  :
            dataframe = dataframe.rename({"buy": "enter_long", "buy_tag": "enter_tag","sell":"enter_short","sell_tag":""}, axis="columns")
        elif "enter_short" not in dataframe.columns:
            dataframe = dataframe.rename({"buy": "enter_long", "buy_tag": "enter_tag","sell":"enter_short","sell_tag":""}, axis="columns")
        
        last_index = dataframe.index[-1]
        if side == "long" and "enter_long" in dataframe.columns and dataframe.at[last_index,"enter_long"] == 1:
            return None
        if side == "short" and "enter_short"  in dataframe.columns and  dataframe.at[last_index,"enter_short"] == 1:
            return None
        
        if side == "long":
            dataframe.at[last_index,"enter_long"] =1
        else:
            dataframe.at[last_index,"enter_short"]=1
        dataframe.at[last_index,"enter_tag"] = tag
       
        # Test if seen this pair and last candle before.
        # always run if process_only_new_candles is set to false
     
        candle_type = self.config.get("candle_type_def", CandleType.SPOT)
        self.dp._set_cached_df(pair, self.timeframe, dataframe, candle_type=candle_type)
        self.dp._emit_df((pair, self.timeframe, candle_type), dataframe, True)

        return dataframe
    def _loop_entry(
        self,pair:str,timestamp:datetime,
        df :DataFrame,
        **kwargs
    ) -> tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
        """
        wrapper around adjust_trade_position to handle the return value
        """
        resp = strategy_safe_wrapper(
            self.loop_entry, default_retval=(None, ""), supress_error=True
        )(
           pair = pair,timestamp = timestamp,
            **kwargs,
        )
        
        result = None
        match resp:
            case (side, stake_amount, price, order_tag):
                result=( side, stake_amount, price, order_tag)
            case (side, stake_amount, price_or_tag):
                if isinstance(price_or_tag, str):
                    result=( side, stake_amount, None, price_or_tag)
                else:
                    result=( side, stake_amount, price_or_tag, "")
            case (side, stake_amount):
                result=( side, stake_amount, None, "")
            case _:
                return None
        
        self.cache_dataframe(dataframe=df,pair=pair,side = side,tag=result[3])
        return result
    
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
    ) -> tuple[float | None, float,str]:
        """
        wrapper around adjust_trade_position to handle the return value
        """
        resp = strategy_safe_wrapper(
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
        resp_tuple = resp if isinstance(resp, tuple) else (resp,)
        match resp_tuple:
            case (stake_amount, price, order_tag):
                result=( stake_amount, price, order_tag)
            case (stake_amount, price_or_tag):
                if isinstance(price_or_tag, str):
                    result=(stake_amount, current_rate, price_or_tag)
                else:
                    result=(stake_amount, price_or_tag, "")
            case (stake_amount, ):
                result=( stake_amount, current_rate, "")
            case _:
                return None
        return result
    def get_latest_candle(
        self,
        pair: str,
        timeframe: str,
        dataframe: DataFrame,
    ) -> tuple[DataFrame | None, datetime | None]:
        """
        Calculates current signal based based on the entry order or exit order
        columns of the dataframe.
        Used by Bot to get the signal to enter, or exit
        :param pair: pair in format ANT/BTC
        :param timeframe: timeframe to use
        :param dataframe: Analyzed dataframe to get signal from.
        :return: (None, None) or (Dataframe, latest_date) - corresponding to the last candle
        """
        if not isinstance(dataframe, DataFrame) or dataframe.empty:
            logger.warning(f"Empty candle (OHLCV) data for pair {pair}")
            return None, None

        try:
            latest_date_pd = dataframe["date"].max()
            latest = dataframe.loc[dataframe["date"] == latest_date_pd].iloc[-1]
        except Exception as e:
            logger.warning(f"Unable to get latest candle (OHLCV) data for pair {pair} - {e}")
            return None, None
        # Explicitly convert to datetime object to ensure the below comparison does not fail
        latest_date: datetime = latest_date_pd.to_pydatetime()

        # Check if dataframe is out of date
        timeframe_minutes = timeframe_to_minutes(timeframe)
        offset = self.config.get("exchange", {}).get("outdated_offset", 1)
        if latest_date < (dt_now() - timedelta(minutes=timeframe_minutes * 2 + offset)):
            raise TimeoutError(
                "Outdated history for pair %s. Last tick is %s minutes old",
                pair,
                int((dt_now() - latest_date).total_seconds() // 60),
            )
        return latest, latest_date