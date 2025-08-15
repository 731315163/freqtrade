from pathlib import Path
from datetime import datetime, timedelta
from freqtradeutil.tradebot import TradeBot


def download_file(userdata: Path, down_since: datetime, erase=False):
    bot = TradeBot(user_data_path=userdata)
    pairs_p = userdata / "pairs.json"
    conf_p = userdata / "config.json"
    bot.download(
        start_date=down_since,
        timeframe=timedelta(days=1),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
    bot.download(
        start_date=down_since,
        timeframe=timedelta(hours=1),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
    bot.download(
        start_date=down_since,
        timeframe=timedelta(minutes=15),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
    bot.download(
        start_date=down_since,
        timeframe=timedelta(minutes=10),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
    bot.download(
        start_date=down_since,
        timeframe=timedelta(minutes=5),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
    bot.download(
        start_date=down_since,
        timeframe=timedelta(minutes=1),
        pairs=pairs_p,
        configpath=conf_p,
        erase=erase,
    )
