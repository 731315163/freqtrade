from enum import IntFlag

from freqtrade.enums import MarketDirection, SignalDirection


class TradeDirection(IntFlag):
    NONE = 0
    SHORT = 1
    LONG = 2
    BOTH = LONG | SHORT
    @staticmethod
    def convert(direction:MarketDirection|str|SignalDirection|None):
        if isinstance( direction, MarketDirection|SignalDirection) :
            direction = direction.value
        match direction:
            case "long":
                return TradeDirection.LONG
            case "short":
                return TradeDirection.SHORT
            case "*":
                return TradeDirection.BOTH
            case _:
                return TradeDirection.NONE
    def __str__(self) -> str :
        # convert to string
        return self.name # type: ignore


