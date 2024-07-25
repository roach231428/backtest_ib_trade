from datetime import datetime
from typing import Dict, List

import pandas as pd
import yfinance as yf

from .base import DataGrabberBase


class YahooFinanceGrabber(DataGrabberBase):
    _max_period = {
        "1m": "7d",
        "2m": "7d",
        "5m": "60d",
        "15m": "60d",
        "30m": "60d",
        "60m": "730d",
        "90m": "60d",
        "1h": "730d",
    }

    def __init__(
        self, tickers: str | List[str], interval: str = "1m", period: str = "max"
    ) -> None:
        super().__init__(tickers, interval=interval, period=period)

    def getHistoricalData(
        self,
        tickers: str | List[str] = None,
        interval: str = None,
        period: str = None,
        start: datetime = None,
        end: datetime = None,
    ) -> pd.DataFrame:
        interval = self.interval if interval is None else interval
        period = self.period if period is None else period
        if period == "max" and interval in self._max_period:
            period = self._max_period[interval]
        period_text = f"last {period[:-1]} days" if period != "max" else "maximum days"
        ticker_msg = (
            self.tickers if isinstance(self.tickers, str) else ", ".join(self.tickers)
        )
        self.logger.info(f"Getting {ticker_msg} {self.interval} {period_text} data...")
        return yf.download(
            self.tickers,
            interval=interval,
            period=period,
            start=start,
            end=end,
            progress=False,
        )
