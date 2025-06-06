from enum import  IntFlag

from freqtrade.enums import MarketDirection, SignalDirection


class LongShort(IntFlag):
    NONE = 0
    LONG = 1
    SHORT = 2
    BOTH = LONG | SHORT
    @staticmethod
    def convert(direction:MarketDirection|str|SignalDirection):
        if isinstance( direction, MarketDirection|SignalDirection) :
            direction = direction.value
        match direction:
            case "long":
                return LongShort.LONG
            case "short":
                return LongShort.SHORT
            case "*":
                return LongShort.BOTH
            case _:
                return LongShort.NONE


