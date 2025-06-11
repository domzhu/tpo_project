import requests
import pandas as pd
from datetime import datetime, timedelta, timezone


def fetch_binance_data(symbol="BTCUSDT", interval="1m", days=4):
    """Fetch last `days` full UTC days plus today's data from Binance."""
    end_time = datetime.now(timezone.utc)
    start_day = end_time.date() - timedelta(days=days)
    start_time = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)

    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current = start_time

    while current < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(current.timestamp() * 1000),
            "limit": 1000,
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        klines = response.json()
        if not klines:
            break
        all_klines.extend(klines)
        last_open = klines[-1][0] / 1000
        current = datetime.fromtimestamp(last_open, tz=timezone.utc) + timedelta(minutes=1)

    df = pd.DataFrame(
        all_klines,
        columns=[
            "open_time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["symbol", "datetime", "Open", "High", "Low", "Close", "Volume"]]
    return df
