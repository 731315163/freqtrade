"""
This module loads custom exchanges
"""

import logging
from inspect import isclass
from typing import Any
from types import MethodType
from freqtrade.enums.candletype import CandleType

from freqtrade.constants import Config, ExchangeConfig

from freqtrade.resolvers.iresolver import IResolver


from freqtrade.exchange import MAP_EXCHANGE_CHILDCLASS, Exchange
import freqtrade.exchange as exchanges
logger = logging.getLogger(__name__)

def _now_is_time_to_refresh_trades(
            self, pair: str, timeframe: str, candle_type: CandleType
        ) -> bool:  # Timeframe in seconds
            logger.info("----------------------------------------class reject _now_is_time_to_refresh_trades True")
            return True
            # trades = self.trades((pair, timeframe, candle_type), False)
            # pair_last_refreshed = int(trades.iloc[-1]["timestamp"])
            # full_candle = (
            #     int(timeframe_to_next_date(timeframe, dt_from_ts(pair_last_refreshed)).timestamp())
            #     * 1000
            # )
Exchange._now_is_time_to_refresh_trades = _now_is_time_to_refresh_trades
class ExchangeResolver(IResolver):
    """
    This class contains all the logic to load a custom exchange class
    """

    object_type = Exchange

    @staticmethod
    def load_exchange(
        config: Config,
        *,
        exchange_config: ExchangeConfig | None = None,
        validate: bool = True,
        load_leverage_tiers: bool = False,
    ) -> Exchange:
        """
        Load the custom class from config parameter
        :param exchange_name: name of the Exchange to load
        :param config: configuration dictionary
        """
        exchange_name: str = config["exchange"]["name"]
        # Map exchange name to avoid duplicate classes for identical exchanges
        exchange_name = MAP_EXCHANGE_CHILDCLASS.get(exchange_name, exchange_name)
        exchange_name = exchange_name.title()
        exchange = None
        try:
            exchange = ExchangeResolver._load_exchange(
                exchange_name,
                kwargs={
                    "config": config,
                    "validate": validate,
                    "exchange_config": exchange_config,
                    "load_leverage_tiers": load_leverage_tiers,
                },
            )
        except ImportError:
            logger.info(
                f"No {exchange_name} specific subclass found. Using the generic class instead."
            )
        if not exchange:
            exchange = Exchange(
                config,
                validate=validate,
                exchange_config=exchange_config,
            )
        return exchange

    @staticmethod
    def _load_exchange(exchange_name: str, kwargs: dict) -> Exchange:
        """
        Loads the specified exchange.
        Only checks for exchanges exported in freqtrade.exchanges
        :param exchange_name: name of the module to import
        :return: Exchange instance or None
        """
        
        def _now_is_time_to_refresh_trades(
            self, pair: str, timeframe: str, candle_type: CandleType
        ) -> bool:  # Timeframe in seconds
            logger.info("instance reject _now_is_time_to_refresh_trades True")
            return True
        try:
            ex_class = getattr(exchanges, exchange_name)

            exchange = ex_class(**kwargs)
            if exchange:
                logger.info(f"Using resolved exchange '{exchange_name}'...")
                # exchange._now_is_time_to_refresh_trades = MethodType(_now_is_time_to_refresh_trades, exchange)
                exchange._now_is_time_to_refresh_trades("", "", CandleType.FUTURES)
                return exchange
        except AttributeError:
            # Pass and raise ImportError instead
            pass

        raise ImportError(
            f"Impossible to load Exchange '{exchange_name}'. This class does not exist "
            "or contains Python code errors."
        )

    @classmethod
    def search_all_objects(
        cls, config: Config, enum_failed: bool, recursive: bool = False
    ) -> list[dict[str, Any]]:
        """
        Searches for valid objects
        :param config: Config object
        :param enum_failed: If True, will return None for modules which fail.
            Otherwise, failing modules are skipped.
        :param recursive: Recursively walk directory tree searching for strategies
        :return: List of dicts containing 'name', 'class' and 'location' entries
        """
        result = []
        for exchange_name in dir(exchanges):
            exchange = getattr(exchanges, exchange_name)
            if isclass(exchange) and issubclass(exchange, Exchange):
                result.append(
                    {
                        "name": exchange_name,
                        "class": exchange,
                        "location": exchange.__module__,
                        "location_rel: ": exchange.__module__.replace("freqtrade.", ""),
                    }
                )
        return result
