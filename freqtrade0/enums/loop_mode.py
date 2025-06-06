from enum import  IntFlag



class LoopMode(IntFlag):
    NONE = 0
    Tick = 1
    NewCandle = 2
    BOTH = NewCandle | Tick
   