from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


def yf_download(tickers, start=None, end=None, interval="1d", **kwargs):
    if start is not None:
        end = datetime.now() if end is None else datetime.strptime(end, "%Y-%m-%d")
        if isinstance(start, str):
            start = datetime.strptime(start, "%Y-%m-%d")
        elif not isinstance(start, datetime):
            return pd.DataFrame()

        if interval in {"1m"}:
            time_list = [
                (x, x + timedelta(days=7))
                for x in pd.date_range(start=start, end=end, freq="7D")
            ]
        elif interval in {"2m", "5m", "15m", "30m", "90m"}:
            time_list = [
                (x, x + timedelta(days=60))
                for x in pd.date_range(start=start, end=end, freq="60D")
            ]
        elif interval in {"60m", "1h"}:
            time_list = [
                (x, x + timedelta(days=730))
                for x in pd.date_range(start=start, end=end, freq="730D")
            ]
        else:
            time_list = [(start, end)]

        res = [
            yf.download(
                tickers,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                interval=interval,
                **kwargs
            )
            for (start, end) in time_list
        ]

    else:
        res = [yf.download(tickers, start=start, end=end, interval=interval, **kwargs)]

    return (
        pd.concat(res).sort_values("Datetime").drop_duplicates()
        if len(res) > 0
        else pd.DataFrame()
    )
