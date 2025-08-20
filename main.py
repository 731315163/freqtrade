from datetime import datetime, timedelta
from pathlib import Path

from freqtrade0 import Runer
 
userd = Path.cwd() / "user_data"
start = datetime(2025, 1, 1)
end = datetime(2025, 1, 10)

strategy =("MartingaleStrategy",timedelta(minutes=5))
# strategy =("RSIMartingaleStrategy",timedelta(minutes=5))
# strategy =("Pin_Strtegy",timedelta(minutes=1))
tradebot = Runer( 
    user_data_path=userd,
    strategy_name=strategy[0],
    timeframe=strategy[1],
    configpath=userd / "config.json",
)


if __name__ == '__main__':
    # download_file(userdata=userd, down_since=datetime(2023, 1, 1))
    # test()
    # tradebot.backtesting(start=start,end=end,timeframe=strategy[1])
    tradebot.webserver(configpath=userd / "config.json")
    # tradebot.lookahead_analysis(start=start)
    # tradebot.recursive_analysis(start=start)
    # tradebot.hyperparameter_optimize(space=[hp.buy], start=start,end=end, timeframe=timedelta(minutes=5), epochs=36)
