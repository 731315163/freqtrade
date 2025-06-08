from freqtrade0.run import Runer
from freqtrade0.strategy.interface import IStrategy
import freqtrade
import freqtrade.enums.candletype


from freqtrade.exchange import Exchange

def _now_is_time_to_refresh_trades(
        self, pair: str, timeframe: str, candle_type: freqtrade.enums.candletype.CandleType
    ) -> bool:  # Timeframe in seconds
        trades = self.trades((pair, timeframe, candle_type), False)
        return True
        pair_last_refreshed = int(trades.iloc[-1]["timestamp"])
        full_candle = (
            int(timeframe_to_next_date(timeframe, dt_from_ts(pair_last_refreshed)).timestamp())
            * 1000
        )
        now = dt_ts()
        return full_candle <= now
Exchange._now_is_time_to_refresh_trades = _now_is_time_to_refresh_trades
Exchange.reject= True